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
	return streamFramePool.Get().(*StreamFrame)
}

// GetDatagramFrame returns a DatagramFrame from the shared pool. The frame's
// Data buffer has capacity protocol.MaxPacketBufferSize and must be re-sliced
// before use. Return the frame with PutDatagramFrame once it has been packed.
func GetDatagramFrame() *DatagramFrame {
	return datagramFramePool.Get().(*DatagramFrame)
}

// PutDatagramFrame returns a pooled DatagramFrame and its Data buffer to the
// pool. Frames not originating from the pool are ignored.
func PutDatagramFrame(f *DatagramFrame) {
	if !f.fromPool {
		return
	}
	if cap(f.Data) != protocol.MaxPacketBufferSize {
		return
	}
	f.Data = f.Data[:0]
	f.DataLenPresent = false
	datagramFramePool.Put(f)
}

func putStreamFrame(f *StreamFrame) {
	if !f.fromPool {
		return
	}
	if cap(f.Data) != protocol.MaxPacketBufferSize {
		panic("wire.PutStreamFrame called with packet of wrong size!")
	}
	f.Data = f.Data[:0]
	streamFramePool.Put(f)
}
