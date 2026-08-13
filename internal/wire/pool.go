package wire

import (
	"github.com/olicesx/quic-go/internal/protocol"
)

// maxDatagramFramePoolLen bounds how many DATAGRAM frames are retained for
// reuse. 1024 x 1200B = ~1.2MB worst-case steady-state retention.
const maxDatagramFramePoolLen = 1024

// datagramFramePool recycles DATAGRAM frames with the same bounded channel
// pattern as streamFramePool: sync.Pool is cleared on every GC cycle, which
// under GC pressure leaves the pool permanently empty and turns every frame
// into a fresh allocation, driving the GC harder.
var datagramFramePool = newDatagramFramePool()

type datagramFramePoolT struct {
	ch chan *DatagramFrame
}

func newDatagramFramePool() *datagramFramePoolT {
	p := &datagramFramePoolT{ch: make(chan *DatagramFrame, maxDatagramFramePoolLen)}
	// warm the pool so the first bursts don't all allocate
	for i := 0; i < maxDatagramFramePoolLen/4; i++ {
		p.ch <- newPooledDatagramFrame()
	}
	return p
}

func newPooledDatagramFrame() *DatagramFrame {
	return &DatagramFrame{
		Data:     make([]byte, 0, MaxDatagramSize),
		fromPool: true,
	}
}

func (p *datagramFramePoolT) Get() *DatagramFrame {
	select {
	case f := <-p.ch:
		return f
	default:
		return newPooledDatagramFrame()
	}
}

func (p *datagramFramePoolT) Put(f *DatagramFrame) {
	select {
	case p.ch <- f:
	default:
		// pool full: drop the frame, GC reclaims it
	}
}

// maxStreamFramePoolLen bounds how many STREAM frames are retained for reuse.
// Sized for high-concurrency proxies: in-flight frames across many
// connections exceed small pool sizes, and every miss is a fresh 1452B
// allocation that feeds the GC loop. 1024 x 1452B = ~1.5MB steady state.
const maxStreamFramePoolLen = 1024

// streamFramePool recycles STREAM frames. A bounded channel pool is used
// instead of sync.Pool: sync.Pool is cleared on every GC cycle, and under GC
// pressure the pool stays empty so every frame re-allocates its
// MaxPacketBufferSize buffer, which drives the GC harder (allocation spiral).
// A channel pool is allocation-free on the hot path, survives GC, and is
// capped so a burst cannot pin unbounded memory.
var streamFramePool = newStreamFramePool()

type streamFramePoolT struct {
	ch chan *StreamFrame
}

func newStreamFramePool() *streamFramePoolT {
	p := &streamFramePoolT{ch: make(chan *StreamFrame, maxStreamFramePoolLen)}
	// warm the pool so the first bursts don't all allocate
	for i := 0; i < maxStreamFramePoolLen/4; i++ {
		p.ch <- newPooledStreamFrame()
	}
	return p
}

// StreamFramePoolLen returns the number of frames currently held in the pool.
// Exported for tests and operational observability (a pool stuck at its
// warm-up size while frames are being received indicates a frame leak).
func StreamFramePoolLen() int {
	return len(streamFramePool.ch)
}

func newPooledStreamFrame() *StreamFrame {
	return &StreamFrame{
		Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
		fromPool: true,
	}
}

func (p *streamFramePoolT) Get() *StreamFrame {
	select {
	case f := <-p.ch:
		return f
	default:
		return newPooledStreamFrame()
	}
}

func (p *streamFramePoolT) Put(f *StreamFrame) {
	select {
	case p.ch <- f:
	default:
		// pool full: drop the frame, GC reclaims it
	}
}

func GetStreamFrame() *StreamFrame {
	return streamFramePool.Get()
}

// GetDatagramFrame returns a DatagramFrame from the shared pool. The frame's
// Data buffer has capacity MaxDatagramSize and must be re-sliced before use.
// Return the frame with PutDatagramFrame once it has been packed.
func GetDatagramFrame() *DatagramFrame {
	return datagramFramePool.Get()
}

// PutDatagramFrame returns a pooled DatagramFrame (and its Data buffer) to
// the pool. Frames not originating from the pool are ignored.
func PutDatagramFrame(f *DatagramFrame) {
	if !f.fromPool {
		return
	}
	if protocol.ByteCount(cap(f.Data)) > MaxDatagramSize {
		// Oversized datagram buffer: skip pooling to avoid pinning a large
		// allocation in the pool.
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
	if protocol.ByteCount(cap(f.Data)) != protocol.MaxPacketBufferSize {
		panic("wire.PutStreamFrame called with packet of wrong size!")
	}
	streamFramePool.Put(f)
}
