//go:build darwin || linux || freebsd

package quic

// getGroPacketBuffer hands out 64 KiB buffers for GRO-coalesced datagrams.
// Its only callers are the oob read path, so it carries the same build
// constraints as sys_conn_oob.go.
func getGroPacketBuffer() *packetBuffer {
	buf := groBufferPool.Get().(*packetBuffer)
	buf.refCount = 1
	buf.Data = buf.Data[:0]
	return buf
}
