package quic

import (
	"net"
	"testing"

	"github.com/olicesx/quic-go/internal/utils"

	"github.com/stretchr/testify/require"
)

func TestClosedLocalConnectionUsesFallbackWhenPacketAddrNil(t *testing.T) {
	written := make(chan net.Addr, 1)
	fallback := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 443}
	conn := newClosedLocalConn(func(addr net.Addr, _ packetInfo) { written <- addr }, utils.DefaultLogger, fallback)
	conn.handlePacket(receivedPacket{remoteAddr: nil})
	select {
	case got := <-written:
		require.Equal(t, fallback, got)
	default:
		t.Fatal("expected CONNECTION_CLOSE to use fallback remote")
	}
}

func TestClosedLocalConnectionPrefersPacketAddrOverFallback(t *testing.T) {
	written := make(chan net.Addr, 1)
	fallback := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 443}
	packetAddr := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 20), Port: 443}
	conn := newClosedLocalConn(func(addr net.Addr, _ packetInfo) { written <- addr }, utils.DefaultLogger, fallback)
	conn.handlePacket(receivedPacket{remoteAddr: packetAddr})
	select {
	case got := <-written:
		require.Equal(t, packetAddr, got)
	default:
		t.Fatal("expected CONNECTION_CLOSE to use the packet source")
	}
}

func TestClosedLocalConnection(t *testing.T) {
	written := make(chan net.Addr, 1)
	conn := newClosedLocalConn(func(addr net.Addr, _ packetInfo) { written <- addr }, utils.DefaultLogger, nil)
	addr := &net.UDPAddr{IP: net.IPv4(127, 1, 2, 3), Port: 1337}
	for i := 1; i <= 20; i++ {
		conn.handlePacket(receivedPacket{remoteAddr: addr})
		if i == 1 || i == 2 || i == 4 || i == 8 || i == 16 {
			select {
			case gotAddr := <-written:
				require.Equal(t, addr, gotAddr) // receive the CONNECTION_CLOSE
			default:
				t.Fatal("expected to receive address")
			}
		} else {
			select {
			case gotAddr := <-written:
				t.Fatalf("unexpected address received: %v", gotAddr)
			default:
				// Nothing received, which is expected
			}
		}
	}
}
