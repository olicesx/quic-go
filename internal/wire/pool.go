package wire

import (
	"sync"

	"github.com/olicesx/quic-go/internal/protocol"
)

// Both frame pools use sync.Pool, which releases everything it holds once an
// object has survived two GC cycles. A bounded channel pool was tried before
// (5a257ebe) and removed again (0d8eb622); re-measuring both variants on a
// 12-thread amd64 host with the real frame lifecycle (fill a 1200-byte STREAM
// frame, pack it with StreamFrame.Append, release it) showed the concern does
// not hold. STREAM-frame numbers, ns/op:
//
//	                            sync.Pool        channel pool
//	hot path, 1 goroutine       35.6-38.4        60-73
//	hot path, 12 goroutines     9.5-13.1         224-262
//	re-allocation rate          0.00001%-1.6%    0%
//
// The re-allocation rate was measured with two forced GCs per 64 frames, i.e.
// the cadence that releases a sync.Pool outright: the pool is not "permanently
// empty under GC pressure", and the channel pool pays a ~1.8x single-thread and
// ~20x multi-thread penalty (one shared channel serialises every P) plus a fixed
// ~1.5MB retention per pool to avoid at most 1.6% of frame allocations. Keep
// sync.Pool; re-run internal/wire's BenchmarkFramePoolAB before revisiting this.
var datagramFramePool = sync.Pool{
	New: func() any {
		return &DatagramFrame{
			Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
			fromPool: true,
		}
	},
}

var streamFramePool = sync.Pool{
	New: func() any {
		return &StreamFrame{
			Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
			fromPool: true,
		}
	},
}

func GetStreamFrame() *StreamFrame {
	f := streamFramePool.Get().(*StreamFrame)
	// Re-arm pool ownership: putStreamFrame clears it when a frame is
	// returned, so a recycled frame always starts out as pool-owned.
	f.fromPool = true
	return f
}

// GetDatagramFrame returns a DatagramFrame from the shared pool. The frame's
// Data buffer has capacity protocol.MaxPacketBufferSize and must be re-sliced
// before use. Return the frame with PutDatagramFrame once it has been packed.
func GetDatagramFrame() *DatagramFrame {
	f := datagramFramePool.Get().(*DatagramFrame)
	f.fromPool = true
	return f
}

// PutDatagramFrame returns a pooled DatagramFrame and its Data buffer to the
// pool. Frames not originating from the pool are ignored. Ownership is
// consumed: a second PutDatagramFrame for the same frame is a no-op unless the
// frame was handed out again in between, so a stray double-return of a frame
// still held by the caller cannot put the same pointer into the pool twice.
func PutDatagramFrame(f *DatagramFrame) {
	if !f.fromPool {
		return
	}
	if cap(f.Data) != protocol.MaxPacketBufferSize {
		return
	}
	f.fromPool = false
	f.Data = f.Data[:0]
	f.DataLenPresent = false
	datagramFramePool.Put(f)
}

func putStreamFrame(f *StreamFrame) {
	if !f.fromPool {
		return
	}
	if cap(f.Data) != protocol.MaxPacketBufferSize {
		panic("wire: StreamFrame.PutBack called with buffer of wrong capacity")
	}
	// Consume ownership before the hand-back. Clearing the flag here makes a
	// back-to-back double PutBack a no-op instead of pooling the same pointer
	// twice (two owners, one Data buffer), and lets the connection-layer
	// tests assert exactly-once release on the frame they handed in. Fields
	// are reset so a future GetStreamFrame caller that forgets one cannot
	// inherit stale values, mirroring PutDatagramFrame.
	f.fromPool = false
	f.Data = f.Data[:0]
	f.StreamID = 0
	f.Offset = 0
	f.Fin = false
	f.DataLenPresent = false
	streamFramePool.Put(f)
}
