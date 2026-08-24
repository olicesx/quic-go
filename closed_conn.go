package quic

import (
	"math/bits"
	"net"
	"sync/atomic"

	"github.com/olicesx/quic-go/internal/utils"
)

// A closedLocalConn is a connection that we closed locally.
// When receiving packets for such a connection, we need to retransmit the packet containing the CONNECTION_CLOSE frame,
// with an exponential backoff.
type closedLocalConn struct {
	counter atomic.Uint32
	logger  utils.Logger

	fallbackAddr net.Addr
	sendPacket   func(net.Addr, packetInfo)
}

var _ packetHandler = &closedLocalConn{}

// newClosedLocalConn creates a new closedLocalConn and runs it.
// fallbackAddr is this connection's last known remote (sconn's cached
// peer). skipAddrBatchConn leaves per-packet source addresses nil, so
// close-queue retransmits use this instead of guessing a socket-wide
// dest. A nil packet addr on a server socket is also safe to send to
// this peer: it is the connection that closed, not PacketConn.RemoteAddr().
func newClosedLocalConn(sendPacket func(net.Addr, packetInfo), logger utils.Logger, fallbackAddr net.Addr) packetHandler {
	return &closedLocalConn{
		sendPacket:   sendPacket,
		logger:       logger,
		fallbackAddr: fallbackAddr,
	}
}

func (c *closedLocalConn) handlePacket(p receivedPacket) {
	n := c.counter.Add(1)
	// exponential backoff
	// only send a CONNECTION_CLOSE for the 1st, 2nd, 4th, 8th, 16th, ... packet arriving
	if bits.OnesCount32(n) != 1 {
		return
	}
	c.logger.Debugf("Received %d packets after sending CONNECTION_CLOSE. Retransmitting.", n)
	addr := p.remoteAddr
	if addr == nil {
		addr = c.fallbackAddr
	}
	if addr == nil {
		return
	}
	c.sendPacket(addr, p.info)
}

func (c *closedLocalConn) destroy(error)                              {}
func (c *closedLocalConn) closeWithTransportError(TransportErrorCode) {}

// A closedRemoteConn is a connection that was closed remotely.
// For such a connection, we might receive reordered packets that were sent before the CONNECTION_CLOSE.
// We can just ignore those packets.
type closedRemoteConn struct{}

var _ packetHandler = &closedRemoteConn{}

func newClosedRemoteConn() packetHandler {
	return &closedRemoteConn{}
}

func (c *closedRemoteConn) handlePacket(receivedPacket)                {}
func (c *closedRemoteConn) destroy(error)                              {}
func (c *closedRemoteConn) closeWithTransportError(TransportErrorCode) {}
