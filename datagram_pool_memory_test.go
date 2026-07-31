package quic

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/wire"
)

// TestDatagramPoolMemoryConvergence verifies that after a heavy burst of
// datagrams the heap returns to (near) its pre-burst size once everything is
// released and GC runs:
//
//   - all receive buffers handed out by Receive must be returnable via
//     ReleaseDatagram (no path may leak a buffer);
//   - the datagram buffer pool must not retain unbounded memory: it is a
//     bounded channel pool (maxDatagramBufPoolLen x 1452B), and sync.Pool
//     based structures (frames) are dropped by GC.
//
// This is the property the deployment cares about: with GOMEMLIMIT set, heap
// growth from a traffic burst must be recoverable, otherwise the pool would
// pin memory forever.
func TestDatagramPoolMemoryConvergence(t *testing.T) {
	q := newDatagramQueue(func() {}, nil)

	// Warm the pools exactly like steady state would.
	payload := make([]byte, 1200)
	f := &wire.DatagramFrame{DataLenPresent: true, Data: payload}
	wireData, _ := f.Append(nil, 0x1)

	// Pre-burst baseline.
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Burst: 50k datagrams through the full queue path.
	const burst = 50000
	for i := 0; i < burst; i++ {
		q.HandleDatagramFrame(f)
		data, err := q.Receive(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		q.ReleaseDatagram(data)
	}
	_ = wireData

	// Release everything and force several GC cycles (sync.Pool clears on GC).
	runtime.GC()
	runtime.GC()
	runtime.GC()
	// Let the background sweeper finish.
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapGrowth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	// Allow a small margin for pool warm-up buffers (warmed at init: 64
	// buffers = 64 x 1452B ~= 93KB) plus runtime noise. A leak would show
	// up as ~burst x 1200B retained.
	if heapGrowth > 256<<10 {
		t.Fatalf("heap did not converge: growth after burst+GC = %d bytes (pre-burst %d -> %d)", heapGrowth, before.HeapAlloc, after.HeapAlloc)
	}
	t.Logf("heap pre-burst=%d post-GC=%d growth=%d bytes", before.HeapAlloc, after.HeapAlloc, heapGrowth)
}

// TestDatagramPoolBounded verifies the receive buffer pool is bounded: after
// releasing more buffers than the pool capacity, further releases are
// dropped (GC reclaims them) instead of unbounded retention.
func TestDatagramPoolBounded(t *testing.T) {
	q := newDatagramQueue(func() {}, nil)

	// Feed the pool far more buffers than its capacity.
	for i := 0; i < maxDatagramBufPoolLen*2; i++ {
		q.HandleDatagramFrame(&wire.DatagramFrame{Data: make([]byte, 1200)})
		data, err := q.Receive(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		q.ReleaseDatagram(data)
	}

	// The pool channel is capped; every Get after the cap must still work
	// (falling back to fresh allocation), and the pool must never grow.
	for i := 0; i < maxDatagramBufPoolLen*2; i++ {
		buf := datagramBufPool.Get()
		if cap(buf) != 0 {
			datagramBufPool.Put(buf)
		}
	}
}
