package wire

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
)

// A/B benchmark for the STREAM frame pool. Commit 0d8eb622 replaced the bounded
// channel pool (which survives GC) with sync.Pool, whose contents are dropped
// once every pooled object has survived two GC cycles. The claim under test is
// that sync.Pool "leaves the pool permanently empty under GC pressure and turns
// every frame into a fresh allocation" - this benchmark measures both the
// steady-state (hot) and the bursty (burst + idle GC) usage pattern and reports
// pool misses directly, so the decision is made on numbers instead of comments.

// countablePool is the minimal surface both variants provide.
type countablePool interface {
	Get() *StreamFrame
	Put(*StreamFrame)
}

func newPooledStreamFrame() *StreamFrame {
	return &StreamFrame{
		Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
		fromPool: true,
	}
}

// syncPoolVariant mirrors the production pool: sync.Pool with the same New.
type syncPoolVariant struct {
	misses atomic.Int64
	pool   sync.Pool
}

func newSyncPoolVariant() *syncPoolVariant {
	v := &syncPoolVariant{}
	v.pool.New = func() any {
		v.misses.Add(1)
		return newPooledStreamFrame()
	}
	return v
}

func (v *syncPoolVariant) Get() *StreamFrame { return v.pool.Get().(*StreamFrame) }
func (v *syncPoolVariant) Put(f *StreamFrame) {
	if !f.fromPool || cap(f.Data) != protocol.MaxPacketBufferSize {
		return
	}
	v.pool.Put(f)
}

// channelPoolVariant is the implementation reverted in 0d8eb622: a bounded
// channel warmed at construction. Same ownership rules as the production pool.
type channelPoolVariant struct {
	misses atomic.Int64
	ch     chan *StreamFrame
}

const benchMaxStreamFramePoolLen = 1024

func newChannelPoolVariant() *channelPoolVariant {
	v := &channelPoolVariant{ch: make(chan *StreamFrame, benchMaxStreamFramePoolLen)}
	for range benchMaxStreamFramePoolLen / 4 {
		v.ch <- newPooledStreamFrame()
	}
	return v
}

func (v *channelPoolVariant) Get() *StreamFrame {
	select {
	case f := <-v.ch:
		return f
	default:
		v.misses.Add(1)
		return newPooledStreamFrame()
	}
}

func (v *channelPoolVariant) Put(f *StreamFrame) {
	if !f.fromPool || cap(f.Data) != protocol.MaxPacketBufferSize {
		return
	}
	select {
	case v.ch <- f:
	default:
		// Pool full: let the GC reclaim the frame.
	}
}

func frameCycle(p countablePool, buf []byte, payload []byte, i int) {
	f := p.Get()
	f.StreamID = 4
	f.Offset = protocol.ByteCount(i * 1200)
	f.Data = append(f.Data[:0], payload...)
	f.Fin = false
	packed, err := f.Append(buf[:0], protocol.Version1)
	if err != nil {
		panic(err)
	}
	_ = packed
	p.Put(f)
}

func reportMisses(b *testing.B, v countablePool) {
	switch p := v.(type) {
	case *syncPoolVariant:
		b.ReportMetric(float64(p.misses.Load())/float64(b.N), "misses/op")
	case *channelPoolVariant:
		b.ReportMetric(float64(p.misses.Load())/float64(b.N), "misses/op")
	}
}

func benchVariants() []struct {
	name string
	new  func() countablePool
} {
	return []struct {
		name string
		new  func() countablePool
	}{
		{"sync-pool", func() countablePool { return newSyncPoolVariant() }},
		{"channel-pool", func() countablePool { return newChannelPoolVariant() }},
	}
}

// BenchmarkFramePoolAB runs the frame lifecycle at several GC cadences. burst=0
// keeps the pool hot (no forced GC); a burst of n frames is followed by two GC
// cycles, which is the threshold at which sync.Pool releases everything it held
// (the first GC moves objects to the victim cache, the second drops them).
func BenchmarkFramePoolAB(b *testing.B) {
	for _, burst := range []int{0, 1024, 256, 64} {
		b.Run(fmt.Sprintf("burst-%d", burst), func(b *testing.B) {
			payload := make([]byte, 1200)
			buf := make([]byte, 0, protocol.MaxPacketBufferSize)
			for _, v := range benchVariants() {
				b.Run(v.name, func(b *testing.B) {
					p := v.new()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						frameCycle(p, buf, payload, i)
						if burst > 0 && (i+1)%burst == 0 {
							runtime.GC()
							runtime.GC()
						}
					}
					b.StopTimer()
					reportMisses(b, p)
				})
			}
		})
	}
}

// BenchmarkFramePoolABParallelHot measures the per-op cost under contention on
// every P, with no forced GC: this isolates the channel pool's cost from GC
// amortisation.
func BenchmarkFramePoolABParallelHot(b *testing.B) {
	for _, v := range benchVariants() {
		b.Run(v.name, func(b *testing.B) {
			p := v.new()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				payload := make([]byte, 1200)
				buf := make([]byte, 0, protocol.MaxPacketBufferSize)
				i := 0
				for pb.Next() {
					frameCycle(p, buf, payload, i)
					i++
				}
			})
			b.StopTimer()
			reportMisses(b, p)
		})
	}
}

// BenchmarkFramePoolABParallel runs the bursty pattern on every P, which is the
// concurrent-proxy case the pool comment describes.
func BenchmarkFramePoolABParallel(b *testing.B) {
	const burst = 64
	for _, v := range benchVariants() {
		b.Run(v.name, func(b *testing.B) {
			p := v.new()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				payload := make([]byte, 1200)
				buf := make([]byte, 0, protocol.MaxPacketBufferSize)
				i := 0
				for pb.Next() {
					frameCycle(p, buf, payload, i)
					i++
					if i%burst == 0 {
						runtime.GC()
						runtime.GC()
					}
				}
			})
			b.StopTimer()
			reportMisses(b, p)
		})
	}
}
