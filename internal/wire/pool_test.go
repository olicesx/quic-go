package wire

import (
	"runtime"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"

	"github.com/stretchr/testify/require"
)

func TestGetAndPutStreamFrames(t *testing.T) {
	f := GetStreamFrame()
	putStreamFrame(f)
}

func TestPanicOnPuttingStreamFrameWithWrongCapacity(t *testing.T) {
	f := GetStreamFrame()
	f.Data = []byte("foobar")
	require.Panics(t, func() { putStreamFrame(f) })
}

func TestAcceptStreamFramesNotFromBuffer(t *testing.T) {
	f := &StreamFrame{Data: []byte("foobar")}
	putStreamFrame(f)
	// No assertion needed as we're just checking it doesn't panic
}

// TestStreamFramePoolReuseBeyondReceiveWindow verifies that a pool sized to
// the in-flight frame count serves steady-state traffic from the pool.
// Sized to maxStreamFramePoolLen: with the pool warmed up and the in-flight
// count within capacity, a second round must not allocate fresh frames.
func TestStreamFramePoolReuseBeyondReceiveWindow(t *testing.T) {
	// In-flight count within pool capacity (maxStreamFramePoolLen).
	const inFlight = maxStreamFramePoolLen / 2

	// Warm-up round: everything misses and then gets returned.
	warm := make([]*StreamFrame, 0, inFlight)
	for i := 0; i < inFlight; i++ {
		warm = append(warm, GetStreamFrame())
	}
	for _, f := range warm {
		putStreamFrame(f)
	}
	runtime.GC()

	// Steady-state round: measure fresh allocations while holding all frames.
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	held := make([]*StreamFrame, 0, inFlight)
	for i := 0; i < inFlight; i++ {
		held = append(held, GetStreamFrame())
	}
	runtime.ReadMemStats(&after)
	for _, f := range held {
		putStreamFrame(f)
	}

	alloc := after.TotalAlloc - before.TotalAlloc
	// All hits: only the held slice header, nothing on the heap. Allow 1MiB
	// of slack for runtime noise; a single missed frame costs 1452B.
	require.Less(t, alloc, uint64(1<<20),
		"steady-state round allocated %d bytes: pool is smaller than the receive window in-flight count", alloc)
}

// BenchmarkStreamFramePoolGetPut measures steady-state reuse: with the pool
// at its working size, Get/Put must be allocation-free.
func BenchmarkStreamFramePoolGetPut(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(protocol.MaxPacketBufferSize)
	for i := 0; i < b.N; i++ {
		f := GetStreamFrame()
		putStreamFrame(f)
	}
}
