//go:build !linux && !windows

package quic

import "golang.org/x/net/ipv4"

// skipAddrBatchConn exists as a type only so the oobConn field compiles on
// non-Linux platforms; it is never instantiated there (newBatchConnOrDefault
// always returns x/net's reader).
type skipAddrBatchConn struct{}

// newBatchConnOrDefault: the raw recvmmsg reader is Linux-only; on other
// platforms always use x/net's address-parsing reader.
func newBatchConnOrDefault(c OOBCapablePacketConn) (bc batchConn, skipAddr *skipAddrBatchConn) {
	return ipv4.NewPacketConn(c), nil
}
