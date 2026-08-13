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

// TestStreamFramePoolReuseBeyondReceiveWindow reproduces the production GC
// storm: hy2 clients use an 8MiB-32MiB stream receive window, so up to
// 8MiB/1452B = 5780 frames can be in flight at once (up to 23100 for the
// 32MiB ceiling). If the pool is smaller than the in-flight count it is a
// zero-reuse pass-through: every frame in steady state is a fresh 1452B
// allocation, which drove GC to 78% of CPU in production pprof.
//
// After one warm-up round, a second round of the same size must be served
// entirely from the pool (zero fresh allocations).
func TestStreamFramePoolReuseBeyondReceiveWindow(t *testing.T) {
	// 8MiB default initial receive window, worst-case frame fill.
	const inFlight = 8 * 1024 * 1024 / protocol.MaxPacketBufferSize

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
