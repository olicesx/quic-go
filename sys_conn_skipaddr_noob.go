//go:build !darwin && !linux && !freebsd

package quic

// enableAddrParsing is a no-op on platforms without the oobConn /
// batchConn implementation: there is no batch reader to swap.
func enableAddrParsing(conn rawConn) {}
