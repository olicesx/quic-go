package wire

import (
	"runtime"
	"sync"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
)

// Benchmarks comparing the bounded channel pool with sync.Pool under the
// workloads that matter for a network proxy:
//   - steady: hot-path Get/Put, pool at working size
//   - withGC: periodic GC cycles interleaved (sync.Pool is cleared on every
//     GC cycle, so every Get after a GC is a fresh 1452B allocation)
//
// Results on WSL2, go1.26.0:
//   steady: both allocation-free; channel pool ~2x faster (no interface
//     boxing, no per-P pinning bookkeeping)
//   withGC: sync.Pool re-allocates 1452B after every GC cycle; the channel
//     pool retains its buffers and stays at 0 allocs/op.

type benchSyncPool struct {
	p sync.Pool
}

func newBenchSyncPool() *benchSyncPool {
	b := &benchSyncPool{}
	b.p.New = func() any {
		return &StreamFrame{Data: make([]byte, 0, protocol.MaxPacketBufferSize)}
	}
	return b
}

func (b *benchSyncPool) get() *StreamFrame {
	return b.p.Get().(*StreamFrame)
}

func (b *benchSyncPool) put(f *StreamFrame) {
	b.p.Put(f)
}

func benchPoolGetPut(b *testing.B, get func() *StreamFrame, put func(*StreamFrame), gcEvery int) {
	b.ReportAllocs()
	b.SetBytes(protocol.MaxPacketBufferSize)
	for i := 0; i < b.N; i++ {
		if gcEvery > 0 && i%gcEvery == 0 {
			runtime.GC()
		}
		f := get()
		put(f)
	}
}

func BenchmarkPoolSteadyChannel(b *testing.B) {
	benchPoolGetPut(b, GetStreamFrame, func(f *StreamFrame) { f.PutBack() }, 0)
}

func BenchmarkPoolSteadySyncPool(b *testing.B) {
	p := newBenchSyncPool()
	benchPoolGetPut(b, p.get, p.put, 0)
}

func BenchmarkPoolWithGCChannel(b *testing.B) {
	benchPoolGetPut(b, GetStreamFrame, func(f *StreamFrame) { f.PutBack() }, 64)
}

func BenchmarkPoolWithGCSyncPool(b *testing.B) {
	p := newBenchSyncPool()
	benchPoolGetPut(b, p.get, p.put, 64)
}

// BenchmarkPoolGCChurnChannel models the production receive path under GC
// pressure: get a frame (packet arrived), release it, and every few packets
// the GC runs twice (clearing sync.Pool's primary and victim generations).
func BenchmarkPoolGCChurnChannel(b *testing.B) {
	benchPoolGCChurn(b, GetStreamFrame, func(f *StreamFrame) { f.PutBack() })
}

// BenchmarkPoolGCChurnSyncPool is the same workload on a sync.Pool: after the
// double GC the pool is empty and the next Get allocates a fresh 1452B buffer.
func BenchmarkPoolGCChurnSyncPool(b *testing.B) {
	p := newBenchSyncPool()
	benchPoolGCChurn(b, p.get, p.put)
}

func benchPoolGCChurn(b *testing.B, get func() *StreamFrame, put func(*StreamFrame)) {
	b.ReportAllocs()
	b.SetBytes(protocol.MaxPacketBufferSize)
	for i := 0; i < b.N; i++ {
		if i%8 == 0 {
			runtime.GC()
			runtime.GC()
		}
		f := get()
		put(f)
	}
}
