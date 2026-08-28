package wire

import (
	"sync"

	"github.com/olicesx/quic-go/internal/protocol"
)

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
