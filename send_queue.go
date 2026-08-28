package quic

import (
	"errors"
	"sync"

	"github.com/olicesx/quic-go/internal/protocol"
)

type sender interface {
	Send(p *packetBuffer, gsoSize uint16, ecn protocol.ECN) error
	Run() error
	WouldBlock() bool
	Available() <-chan struct{}
	// RunStopped is closed once the run loop returned, whether cleanly or
	// on a write error. Waiters on Available must also select on it, or a
	// sender death would strand them forever (no further tokens exist).
	RunStopped() <-chan struct{}
	// LastRunError reports the error that terminated the run loop, if any.
	LastRunError() error
	Close()
}

type queueEntry struct {
	buf     *packetBuffer
	gsoSize uint16
	ecn     protocol.ECN
}

var errSendQueueStopped = errors.New("quic: send queue stopped")

type sendQueue struct {
	queue       chan queueEntry
	closeCalled chan struct{} // closed when Close() is called
	runStopped  chan struct{} // closed when the run loop returns
	available   chan struct{}
	conn        sendConn

	stateMu sync.RWMutex
	stopped bool // guarded by stateMu; ownership transfer stops before drain

	errMu     sync.Mutex
	lastErr   error // the error that terminated Run, set before runStopped closes
	closeOnce sync.Once
}

var _ sender = &sendQueue{}

const sendQueueCapacity = 8

func newSendQueue(conn sendConn) sender {
	return &sendQueue{
		conn:        conn,
		runStopped:  make(chan struct{}),
		closeCalled: make(chan struct{}),
		available:   make(chan struct{}, 1),
		queue:       make(chan queueEntry, sendQueueCapacity),
	}
}

// Send sends out a packet. It's guaranteed to not block.
// Callers need to make sure that there's actually space in the send queue by calling WouldBlock.
// Otherwise Send will panic.
func (h *sendQueue) Send(p *packetBuffer, gsoSize uint16, ecn protocol.ECN) error {
	h.stateMu.RLock()
	defer h.stateMu.RUnlock()
	if h.stopped {
		p.Release()
		if err := h.LastRunError(); err != nil {
			return err
		}
		return errSendQueueStopped
	}
	select {
	case h.queue <- queueEntry{buf: p, gsoSize: gsoSize, ecn: ecn}:
		// clear available channel if we've reached capacity
		if len(h.queue) == sendQueueCapacity {
			select {
			case <-h.available:
			default:
			}
		}
		return nil
	default:
		panic("sendQueue.Send would have blocked")
	}
}

func (h *sendQueue) WouldBlock() bool {
	return len(h.queue) == sendQueueCapacity
}

func (h *sendQueue) Available() <-chan struct{} {
	return h.available
}

func (h *sendQueue) RunStopped() <-chan struct{} {
	return h.runStopped
}

func (h *sendQueue) LastRunError() error {
	h.errMu.Lock()
	defer h.errMu.Unlock()
	return h.lastErr
}

func (h *sendQueue) Run() (runErr error) {
	defer func() {
		if runErr != nil {
			h.errMu.Lock()
			h.lastErr = runErr
			h.errMu.Unlock()
		}
		// Exclude Send while transitioning to stopped and draining. Without
		// this boundary, a send can win after drain and leak its buffer.
		h.stateMu.Lock()
		h.stopped = true
		h.drain()
		close(h.runStopped)
		h.stateMu.Unlock()
	}()
	var shouldClose bool
	for {
		if shouldClose && len(h.queue) == 0 {
			return nil
		}
		select {
		case <-h.closeCalled:
			h.closeCalled = nil // prevent this case from being selected again
			// make sure that all queued packets are actually sent out
			shouldClose = true
		case e := <-h.queue:
			err := h.conn.Write(e.buf.Data, e.gsoSize, e.ecn)
			e.buf.Release()
			if err != nil {
				// This additional check enables:
				// 1. Checking for "datagram too large" message from the kernel, as such,
				// 2. Path MTU discovery,and
				// 3. Eventual detection of loss PingFrame.
				if !isSendMsgSizeErr(err) {
					return err
				}
			}
			select {
			case h.available <- struct{}{}:
			default:
			}
		}
	}
}

func (h *sendQueue) drain() {
	for {
		select {
		case e := <-h.queue:
			e.buf.Release()
		default:
			return
		}
	}
}

func (h *sendQueue) Close() {
	h.closeOnce.Do(func() { close(h.closeCalled) })
	// wait until the run loop returned
	<-h.runStopped
}
