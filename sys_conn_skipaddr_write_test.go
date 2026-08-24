//go:build linux

package quic

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/utils"
	"github.com/stretchr/testify/require"
)

type fixedRemoteOOBPacketConn struct {
	writtenTo *net.UDPAddr
}

func (*fixedRemoteOOBPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, errors.New("not implemented")
}

func (c *fixedRemoteOOBPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if addr != nil {
		c.writtenTo, _ = addr.(*net.UDPAddr)
	}
	return len(b), nil
}

func (*fixedRemoteOOBPacketConn) Close() error                          { return nil }
func (*fixedRemoteOOBPacketConn) LocalAddr() net.Addr                   { return &net.UDPAddr{} }
func (*fixedRemoteOOBPacketConn) SetDeadline(time.Time) error           { return nil }
func (*fixedRemoteOOBPacketConn) SetReadDeadline(time.Time) error       { return nil }
func (*fixedRemoteOOBPacketConn) SetWriteDeadline(time.Time) error      { return nil }
func (*fixedRemoteOOBPacketConn) SetReadBuffer(int) error               { return nil }
func (*fixedRemoteOOBPacketConn) SyscallConn() (syscall.RawConn, error) { return nil, nil }
func (*fixedRemoteOOBPacketConn) ReadMsgUDP([]byte, []byte) (int, int, int, *net.UDPAddr, error) {
	return 0, 0, 0, nil, errors.New("not implemented")
}

func (c *fixedRemoteOOBPacketConn) WriteMsgUDP(b, _ []byte, addr *net.UDPAddr) (int, int, error) {
	c.writtenTo = addr
	return len(b), 0, nil
}

func TestClosedLocalConnRetransmitWithSkippedSourceAddr(t *testing.T) {
	remote := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 443}
	packetConn := &fixedRemoteOOBPacketConn{}
	raw := &oobConn{OOBCapablePacketConn: packetConn, skipAddr: &skipAddrBatchConn{}}
	var writeErr error
	closed := newClosedLocalConn(func(addr net.Addr, info packetInfo) {
		_, writeErr = raw.WritePacket([]byte("close"), addr, info.OOB(), 0, protocol.ECNUnsupported)
	}, utils.DefaultLogger, remote)

	require.NotPanics(t, func() {
		closed.handlePacket(receivedPacket{remoteAddr: nil})
	})
	require.NoError(t, writeErr)
	require.Equal(t, remote, packetConn.writtenTo)
}

func TestWritePacketDropsNilAddrWithoutGuessing(t *testing.T) {
	packetConn := &fixedRemoteOOBPacketConn{}
	raw := &oobConn{OOBCapablePacketConn: packetConn, skipAddr: &skipAddrBatchConn{}}
	require.NotPanics(t, func() {
		n, err := raw.WritePacket([]byte("close"), nil, nil, 0, protocol.ECNUnsupported)
		require.NoError(t, err)
		require.Zero(t, n)
	})
	require.Nil(t, packetConn.writtenTo)
}

func TestWritePacketStillPanicsOnUnexpectedAddrType(t *testing.T) {
	packetConn := &fixedRemoteOOBPacketConn{}
	raw := &oobConn{OOBCapablePacketConn: packetConn, skipAddr: &skipAddrBatchConn{}}
	require.Panics(t, func() {
		_, _ = raw.WritePacket([]byte("close"), &net.TCPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 443}, nil, 0, protocol.ECNUnsupported)
	})
}

func TestClosedLocalConnDropsNilWhenFallbackMissing(t *testing.T) {
	var sent net.Addr
	closed := newClosedLocalConn(func(addr net.Addr, _ packetInfo) {
		sent = addr
	}, utils.DefaultLogger, nil)
	require.NotPanics(t, func() {
		closed.handlePacket(receivedPacket{remoteAddr: nil})
	})
	require.Nil(t, sent)
}
