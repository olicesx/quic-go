package quic

import (
	"github.com/olicesx/quic-go/internal/protocol"
)

type packetBuffer struct {
	Data []byte

	// refCount counts how many packets Data is used in.
	// It doesn't support concurrent use.
	// It is > 1 when used for coalesced packet.
	refCount int
}

// Split increases the refCount.
// It must be called when a packet buffer is used for more than one packet,
// e.g. when splitting coalesced packets.
func (b *packetBuffer) Split() {
	b.refCount++
}

// Decrement decrements the reference counter.
// It doesn't put the buffer back into the pool.
func (b *packetBuffer) Decrement() {
	b.refCount--
	if b.refCount < 0 {
		panic("negative packetBuffer refCount")
	}
}

// MaybeRelease puts the packet buffer back into the pool,
// if the reference counter already reached 0.
func (b *packetBuffer) MaybeRelease() {
	// only put the packetBuffer back if it's not used any more
	if b.refCount == 0 {
		b.putBack()
	}
}

// Release puts back the packet buffer into the pool.
// It should be called when processing is definitely finished.
func (b *packetBuffer) Release() {
	b.Decrement()
	if b.refCount != 0 {
		panic("packetBuffer refCount not zero")
	}
	b.putBack()
}

// Len returns the length of Data
func (b *packetBuffer) Len() protocol.ByteCount { return protocol.ByteCount(len(b.Data)) }
func (b *packetBuffer) Cap() protocol.ByteCount { return protocol.ByteCount(cap(b.Data)) }

func (b *packetBuffer) putBack() {
	if cap(b.Data) == protocol.MaxPacketBufferSize {
		bufferPool.Put(b)
		return
	}
	if cap(b.Data) == protocol.MaxLargePacketBufferSize {
		largeBufferPool.Put(b)
		return
	}
	panic("putPacketBuffer called with packet of wrong size!")
}

// maxPacketBufferPoolLen bounds how many packet buffers are retained for
// reuse. It must cover the buffers in flight between ReadPacket and the
// completion of packet processing (batch reads of up to 256 packets, plus
// per-connection processing queues). 4096 x 1452B = ~6MB worst-case.
const maxPacketBufferPoolLen = 4096

// maxLargePacketBufferPoolLen bounds the GSO-sized buffers used for
// sending. 256 x 20KiB = ~5MB worst-case.
const maxLargePacketBufferPoolLen = 256

// packetBufferPool is a bounded channel pool. A sync.Pool is cleared on every
// GC cycle, so under traffic every received packet allocates a fresh 1452B
// packetBuffer, which drives the GC harder (allocation spiral). The channel
// pool is allocation-free on the hot path, survives GC, and is capped.
type packetBufferPool struct {
	ch         chan *packetBuffer
	bufferSize int
}

func newPacketBufferPool(size int, bufferSize int) *packetBufferPool {
	p := &packetBufferPool{
		ch:         make(chan *packetBuffer, size),
		bufferSize: bufferSize,
	}
	// Warm the pool so the first batches don't all allocate.
	warmup := min(size, 1024)
	for i := 0; i < warmup; i++ {
		p.ch <- newPacketBuffer(bufferSize)
	}
	return p
}

func newPacketBuffer(bufferSize int) *packetBuffer {
	return &packetBuffer{Data: make([]byte, 0, bufferSize)}
}

func (p *packetBufferPool) Get() *packetBuffer {
	select {
	case b := <-p.ch:
		return b
	default:
		return newPacketBuffer(p.bufferSize)
	}
}

func (p *packetBufferPool) Put(b *packetBuffer) {
	select {
	case p.ch <- b:
	default:
		// Pool full: drop the buffer, GC reclaims it.
	}
}

var bufferPool = newPacketBufferPool(maxPacketBufferPoolLen, protocol.MaxPacketBufferSize)
var largeBufferPool = newPacketBufferPool(maxLargePacketBufferPoolLen, protocol.MaxLargePacketBufferSize)

func getPacketBuffer() *packetBuffer {
	buf := bufferPool.Get()
	buf.refCount = 1
	buf.Data = buf.Data[:0]
	return buf
}

func getLargePacketBuffer() *packetBuffer {
	buf := largeBufferPool.Get()
	buf.refCount = 1
	buf.Data = buf.Data[:0]
	return buf
}
