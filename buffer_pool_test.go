package quic

import (
	"runtime"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"

	"github.com/stretchr/testify/require"
)

func TestBufferPoolSizes(t *testing.T) {
	buf1 := getPacketBuffer()
	require.Equal(t, protocol.MaxPacketBufferSize, cap(buf1.Data))
	require.Zero(t, buf1.Len())
	buf1.Data = append(buf1.Data, []byte("foobar")...)
	require.Equal(t, protocol.ByteCount(6), buf1.Len())

	buf2 := getLargePacketBuffer()
	require.Equal(t, protocol.MaxLargePacketBufferSize, cap(buf2.Data))
	require.Zero(t, buf2.Len())
}

func TestBufferPoolRelease(t *testing.T) {
	buf1 := getPacketBuffer()
	buf1.Release()
	// panics if released twice
	require.Panics(t, func() { buf1.Release() })

	// panics if wrong-sized buffers are passed
	buf2 := getLargePacketBuffer()
	buf2.Data = make([]byte, 10) // replace the underlying slice
	require.Panics(t, func() { buf2.Release() })
}

func TestBufferPoolSplitting(t *testing.T) {
	buf := getPacketBuffer()
	buf.Split()
	buf.Split()
	// now we have 3 parts
	buf.Decrement()
	buf.Decrement()
	buf.Decrement()
	require.Panics(t, func() { buf.Decrement() })
}

// TestPacketBufferPoolSurvivesGC documents the GC-storm reproduction from
// production: with sync.Pool, every GC cycle clears the pool, so each
// subsequently received QUIC packet allocates a fresh 1452B packetBuffer.
// Under traffic this is a self-sustaining allocation spiral that pushed the
// GC to 78% of CPU. The pool must survive GC: buffers retained before a GC
// cycle must still be available afterwards.
func TestPacketBufferPoolSurvivesGC(t *testing.T) {
	require.Greater(t, len(bufferPool.ch), 0)
	before := len(bufferPool.ch)
	beforeLarge := len(largeBufferPool.ch)
	for i := 0; i < 3; i++ {
		runtime.GC()
	}
	require.Equal(t, before, len(bufferPool.ch), "retained packet buffers must survive GC cycles")
	require.Equal(t, beforeLarge, len(largeBufferPool.ch), "retained large packet buffers must survive GC cycles")
}

// TestPacketBufferPoolBounded verifies the pool never retains more buffers
// than its capacity: releasing far more buffers than the cap must not grow
// the pool unboundedly.
func TestPacketBufferPoolBounded(t *testing.T) {
	const overfill = maxPacketBufferPoolLen * 2
	held := make([]*packetBuffer, 0, overfill)
	for i := 0; i < overfill; i++ {
		b := getPacketBuffer()
		held = append(held, b)
	}
	for _, b := range held {
		b.Release()
	}
	require.LessOrEqual(t, len(bufferPool.ch), maxPacketBufferPoolLen, "pool must stay bounded at its capacity")
	// And every buffer handed out afterwards is usable.
	for i := 0; i < maxPacketBufferPoolLen; i++ {
		b := getPacketBuffer()
		require.Equal(t, protocol.MaxPacketBufferSize, cap(b.Data))
		b.Release()
	}
}

// BenchmarkPacketBufferPoolGetRelease measures steady-state reuse: with the
// pool at its working size, Get/Release must be allocation-free.
func BenchmarkPacketBufferPoolGetRelease(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(protocol.MaxPacketBufferSize)
	for i := 0; i < b.N; i++ {
		buf := getPacketBuffer()
		buf.Release()
	}
}
