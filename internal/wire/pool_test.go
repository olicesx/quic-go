package wire

import (
	"runtime"
	"sync"
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

func TestFramePoolsParallel(t *testing.T) {
	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range 1000 {
				sf := GetStreamFrame()
				require.Equal(t, protocol.MaxPacketBufferSize, cap(sf.Data))
				putStreamFrame(sf)

				df := GetDatagramFrame()
				require.Equal(t, protocol.MaxPacketBufferSize, cap(df.Data))
				PutDatagramFrame(df)
			}
		}()
	}
	wg.Wait()
}

func TestFramePoolsRemainUsableAcrossGC(t *testing.T) {
	sf := GetStreamFrame()
	putStreamFrame(sf)
	df := GetDatagramFrame()
	PutDatagramFrame(df)

	runtime.GC()
	runtime.GC()

	sf = GetStreamFrame()
	require.Equal(t, protocol.MaxPacketBufferSize, cap(sf.Data))
	putStreamFrame(sf)
	df = GetDatagramFrame()
	require.Equal(t, protocol.MaxPacketBufferSize, cap(df.Data))
	PutDatagramFrame(df)
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
