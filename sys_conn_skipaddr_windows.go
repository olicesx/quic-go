//go:build windows

package quic

// enableAddrParsing is a no-op on Windows: there is no oobConn / batchConn
// implementation on this platform (sys_conn_windows.go uses plain
// ReadFrom), so there is no reader to swap.
func enableAddrParsing(conn rawConn) {}
