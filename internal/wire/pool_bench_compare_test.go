package wire

import (
	"runtime"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
)

func benchPoolGetPut(b *testing.B, gcEvery int) {
	b.ReportAllocs()
	b.SetBytes(protocol.MaxPacketBufferSize)
	for i := 0; i < b.N; i++ {
		if gcEvery > 0 && i%gcEvery == 0 {
			runtime.GC()
		}
		f := GetStreamFrame()
		f.PutBack()
	}
}

func BenchmarkStreamFramePoolSteady(b *testing.B) {
	benchPoolGetPut(b, 0)
}

func BenchmarkStreamFramePoolWithGC(b *testing.B) {
	benchPoolGetPut(b, 64)
}

func BenchmarkStreamFramePoolParallel(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(protocol.MaxPacketBufferSize)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f := GetStreamFrame()
			f.PutBack()
		}
	})
}
