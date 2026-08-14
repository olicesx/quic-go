package quic

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/utils"
	"github.com/olicesx/quic-go/internal/utils/ringbuffer"
	"github.com/olicesx/quic-go/internal/wire"
)

const (
	// maxDatagramSendQueueLen bounds the per-connection DATAGRAM send queue.
	// Each QUIC packet packs at most one DATAGRAM frame (packet_packer.go),
	// and the send loop (sendPacketsWithoutGSO) checks SendMode after every
	// packet — if the pacer or cwnd limits sending, the loop returns and
	// remaining datagrams wait. With the upstream default of 32, a burst of
	// UDP relay packets (e.g. game ticks at 60-120 Hz) during concurrent TCP
	// proxy traffic fills the queue and blocks subsequent Add() calls, which
	// delays time-sensitive game heartbeats. 256 gives enough headroom for
	// ~2 seconds of 120 Hz game traffic without blocking.
	maxDatagramSendQueueLen = 256
	// maxDatagramRcvQueueLen bounds the per-connection DATAGRAM receive queue.
	// When full, HandleDatagramFrame silently drops incoming datagrams. Game
	// servers can burst hundreds of UDP packets during explosions / mass
	// player events; 128 was too small for these bursts. 512 absorbs typical
	// FPS game bursts (~1 MB of 2 KB packets) while keeping the per-connection
	// memory footprint bounded.
	maxDatagramRcvQueueLen = 512
	// maxDatagramBufPoolLen bounds how many receive buffers are retained for
	// reuse. Sized for high-concurrency proxies: in-flight datagrams across
	// many connections exceed small pool sizes, and every miss is a fresh
	// 1452B allocation that feeds the GC loop. 1024 x 1452B = ~1.5MB steady
	// state, which still caps the pool's footprint while absorbing line-rate
	// bursts.
	maxDatagramBufPoolLen = 1024
)

// datagramSendQueueFullTimeout bounds how long Add waits on a full send queue
// before dropping the datagram and returning errDatagramQueueFullTimeout. It
// is a var so tests can shorten the wait. The production value sits well above
// a normal backpressure burst (seconds) and below a QUIC peer's idle timeout
// (60s in the hy2 client), so a genuinely stalled transport is surfaced to the
// caller before the connection itself tears down.
var datagramSendQueueFullTimeout = 30 * time.Second

// ErrDatagramQueueFullTimeout is returned by Add when the send queue stayed
// full for datagramSendQueueFullTimeout. The datagram was dropped and the
// connection is still alive; callers may retry with a later datagram.
var ErrDatagramQueueFullTimeout = errors.New("datagram send queue full: timed out")

// datagramBufPool recycles the receive-side datagram buffers. Incoming
// DATAGRAM frames are copied out of the packet buffer into one of these,
// handed to ReceiveDatagram, and returned via ReleaseDatagram. Without the
// pool, every inbound datagram allocates (line-rate UDP relay = constant GC
// pressure); with it, buffers are reused and only the ones actually in flight
// are live.
//
// A bounded channel is used instead of sync.Pool: sync.Pool boxes []byte into
// an interface on every Put/Get (a 24B slice-header escape allocation per
// datagram), and it has no upper bound, so a burst can pin arbitrarily many
// buffers until the next GC. The channel pool is allocation-free and capped.
var datagramBufPool = newDatagramBufPool()

type datagramBufPoolT struct {
	ch chan []byte
}

func newDatagramBufPool() *datagramBufPoolT {
	p := &datagramBufPoolT{ch: make(chan []byte, maxDatagramBufPoolLen)}
	// warm the pool so the first bursts don't all allocate
	for i := 0; i < maxDatagramBufPoolLen/4; i++ {
		p.ch <- make([]byte, 0, protocol.MaxPacketBufferSize)
	}
	return p
}

func (p *datagramBufPoolT) Get() []byte {
	select {
	case b := <-p.ch:
		return b[:0]
	default:
		return make([]byte, 0, protocol.MaxPacketBufferSize)
	}
}

func (p *datagramBufPoolT) Put(b []byte) {
	if cap(b) != protocol.MaxPacketBufferSize {
		// Not one of ours (oversized datagram or caller buffer): let GC
		// reclaim it instead of pinning a large allocation in the pool.
		return
	}
	select {
	case p.ch <- b:
	default:
		// pool full: drop the buffer, GC reclaims it
	}
}

type datagramQueue struct {
	sendMx    sync.Mutex
	sendQueue ringbuffer.RingBuffer[*wire.DatagramFrame]
	sent      chan struct{} // used to notify Add that a datagram was dequeued

	rcvMx    sync.Mutex
	rcvQueue [][]byte
	rcvd     chan struct{} // used to notify Receive that a new datagram was received

	closeErr error
	closed   chan struct{}

	hasData func()

	logger utils.Logger
}

func newDatagramQueue(hasData func(), logger utils.Logger) *datagramQueue {
	return &datagramQueue{
		hasData: hasData,
		rcvd:    make(chan struct{}, 1),
		sent:    make(chan struct{}, 1),
		closed:  make(chan struct{}),
		logger:  logger,
		// Pre-allocate the receive queue so steady-state enqueue never
		// triggers slice growth allocations.
		rcvQueue: make([][]byte, 0, maxDatagramRcvQueueLen),
	}
}

// Add queues a new DATAGRAM frame for sending.
// Up to maxDatagramSendQueueLen DATAGRAM frames will be queued.
// Once that limit is reached, Add blocks until the queue size has reduced or
// datagramSendQueueFullTimeout elapses, whichever comes first. The timeout
// keeps a stalled transport bounded: without it a sender parked on a full
// queue waits forever, which would strand shared dispatcher workers.
func (h *datagramQueue) Add(f *wire.DatagramFrame) error {
	h.sendMx.Lock()

	for {
		if h.sendQueue.Len() < maxDatagramSendQueueLen {
			h.sendQueue.PushBack(f)
			h.sendMx.Unlock()
			h.hasData()
			return nil
		}
		select {
		case <-h.sent: // drain the queue so we don't loop immediately
		default:
		}
		h.sendMx.Unlock()
		timer := time.NewTimer(datagramSendQueueFullTimeout)
		select {
		case <-h.closed:
			// Connection closed while blocked on a full queue: the frame
			// was never sent, return it to the pool.
			timer.Stop()
			wire.PutDatagramFrame(f)
			return h.closeErr
		case <-h.sent:
			timer.Stop()
		case <-timer.C:
			// Queue stayed full for the whole timeout: the transport is
			// stalled, not merely backpressured. Drop this datagram and
			// surface a bounded error instead of parking forever.
			wire.PutDatagramFrame(f)
			return ErrDatagramQueueFullTimeout
		}
		h.sendMx.Lock()
	}
}

// Peek gets the next DATAGRAM frame for sending.
// If actually sent out, Pop needs to be called before the next call to Peek.
func (h *datagramQueue) Peek() *wire.DatagramFrame {
	h.sendMx.Lock()
	defer h.sendMx.Unlock()
	if h.sendQueue.Empty() {
		return nil
	}
	return h.sendQueue.PeekFront()
}

func (h *datagramQueue) Pop() {
	h.sendMx.Lock()
	defer h.sendMx.Unlock()
	_ = h.sendQueue.PopFront()
	select {
	case h.sent <- struct{}{}:
	default:
	}
}

// HandleDatagramFrame handles a received DATAGRAM frame.
func (h *datagramQueue) HandleDatagramFrame(f *wire.DatagramFrame) {
	buf := datagramBufPool.Get()
	if cap(buf) < len(f.Data) {
		// Oversized datagram (larger than the pool cap); allocate fresh and
		// ReleaseDatagram will skip pooling it (cap check).
		buf = make([]byte, len(f.Data))
	} else {
		buf = buf[:len(f.Data)]
	}
	copy(buf, f.Data)
	// The parsed frame is a pooled object: its payload has been copied into
	// our own buffer, so the frame (and its Data) can go back to the pool.
	wire.PutDatagramFrame(f)
	var queued bool
	h.rcvMx.Lock()
	if len(h.rcvQueue) < maxDatagramRcvQueueLen {
		h.rcvQueue = append(h.rcvQueue, buf)
		queued = true
		select {
		case h.rcvd <- struct{}{}:
		default:
		}
	}
	h.rcvMx.Unlock()
	if !queued {
		// Receive queue full: return the buffer to the pool instead of
		// abandoning it for GC. Put's cap check skips non-pooled (oversized)
		// buffers, so this is safe for both pooled and freshly allocated bufs.
		datagramBufPool.Put(buf)
		if h.logger.Debug() {
			h.logger.Debugf("Discarding received DATAGRAM frame (%d bytes payload)", len(f.Data))
		}
	}
}

// ReleaseDatagram returns a datagram previously handed out by Receive back to
// the pool. Callers MUST call this exactly once per datagram after they are
// done with the buffer. It is a no-op for buffers that were not pooled (e.g.
// ones whose size exceeded the pool cap at receive time).
func (h *datagramQueue) ReleaseDatagram(data []byte) {
	if data == nil {
		return
	}
	if cap(data) != protocol.MaxPacketBufferSize {
		// Buffer was not taken from the pool (oversized datagram or a
		// caller-supplied buffer); let GC reclaim it.
		return
	}
	datagramBufPool.Put(data)
}

// Receive gets a received DATAGRAM frame.
func (h *datagramQueue) Receive(ctx context.Context) ([]byte, error) {
	for {
		h.rcvMx.Lock()
		if len(h.rcvQueue) > 0 {
			data := h.rcvQueue[0]
			h.rcvQueue = h.rcvQueue[1:]
			h.rcvMx.Unlock()
			return data, nil
		}
		h.rcvMx.Unlock()
		select {
		case <-h.rcvd:
			continue
		case <-h.closed:
			return nil, h.closeErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (h *datagramQueue) CloseWithError(e error) {
	h.closeErr = e
	close(h.closed)
}
