package wire

import (
	"runtime"
	"sync"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
)

// Benchmarks comparing the bounded channel pool with sync.Pool under two
// synthetic workloads:
//   - steady: hot-path Get/Put with a warm pool
//   - withGC: periodic forced GC cycles while the pool remains active
//
// On Go 1.26, both pools are allocation-free in the steady benchmark, while
// sync.Pool is faster, especially with parallel callers. During GC, sync.Pool
// moves current entries to a victim generation and may discard entries that
// remain unused through a later cycle; it is not cleared unconditionally on
// every GC. The forced-GC timings are dominated by runtime.GC and should be
// read primarily for allocation behavior. They also exclude the channel
// pool's eager warm-up cost and do not model end-to-end natural-GC latency.

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

// BenchmarkPoolGCChurnChannel repeatedly gets and releases a frame, with two
// back-to-back GC cycles every few operations. With no intervening Put, those
// cycles rotate and then discard sync.Pool's current and victim entries.
func BenchmarkPoolGCChurnChannel(b *testing.B) {
	benchPoolGCChurn(b, GetStreamFrame, func(f *StreamFrame) { f.PutBack() })
}

// BenchmarkPoolGCChurnSyncPool is the same workload on a sync.Pool. After the
// back-to-back GC cycles discard an idle entry, the next Get allocates a new
// frame and MaxPacketBufferSize buffer.
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
