//go:build linux

package quic

import (
	"errors"
	"syscall"
	"unsafe"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

// skipAddrBatchConn implements batchConn with a raw recvmmsg loop that never
// parses the datagram source address.
//
// x/net's ipv4.PacketConn.ReadBatch unconditionally installs
// parseFn=parseInetAddr, which allocates a net.IP (16B) plus a *net.UDPAddr
// for every received datagram. In a client deployment the parsed address is
// not needed on the receive hot path (the transport dispatches by connection
// ID; the sendConn caches the dial-time remote), so those two allocations
// per packet are pure churn. The close-queue path is the exception: after a
// local close, closedLocalConn retransmits CONNECTION_CLOSE using
// receivedPacket.remoteAddr, which is nil here. That handler therefore
// uses the connection's cached remote (sconn) instead of the nil packet
// address. oobConn.WritePacket drops a nil dest instead of panicking.
// parseFn can only be suppressed inside x/net, so the only way to skip it is
// to own the mmsghdr plumbing: the layout mirrors x/net's internal/socket
// (mmsghdr = msghdr + u32 len, padded to 64B on amd64), and only N / NN /
// Flags are unpacked. Addr stays nil; ECN/PKTINFO control data still flows
// through the OOB buffer and is parsed by oobConn.ReadPacket.
//
// English: raw recvmmsg batch reader that skips per-packet source-address
// allocation for client (single-remote) listeners.
// 中文：裸 recvmmsg 批量读，面向客户端（单一远端）监听者，跳过每包源地址解析分配。
type skipAddrBatchConn struct {
	conn syscall.RawConn
	// Scratch reused across calls; data pointers are re-bound to the
	// caller's buffers on every ReadBatch.
	hs    []mmsghdrSkip
	names []byte
	vecs  []syscall.Iovec
	// n is the header count bound by the current ReadBatch; got/operr
	// carry syscall results out of the zero-capture readCallback.
	n     int
	got   int
	operr error
}

// mmsghdrSkip mirrors Linux struct mmsghdr: a native struct msghdr followed
// by a u32 total length. On amd64 Go's syscall.Msghdr is 56B with 8B
// alignment, so Len lands at offset 56 and the struct pads to 64B — the
// same layout the kernel expects (verified against x/net's cgo definition).
type mmsghdrSkip struct {
	Hdr syscall.Msghdr
	Len uint32
}

const (
	sizeofSockaddrInet6Skip = 28 // struct sockaddr_in6 on Linux
	skipAddrBatchSize       = 64
)

// msghdrLen adapts an int buffer length to the platform's native
// msghdr.Controllen / Iovlen field type; see msghdrlen_*.go.
func msghdrLen(n int) msghdrLenType { return msghdrLenType(n) }

// newBatchConnOrDefault returns the raw recvmmsg reader (no per-datagram
// source address parsing) for client-side use, or x/net's reader if the
// raw fd is unavailable.
func newBatchConnOrDefault(c OOBCapablePacketConn) (bc batchConn, skipAddr *skipAddrBatchConn) {
	skipAddr, err := newSkipAddrBatchConn(c)
	if err == nil {
		return skipAddr, skipAddr
	}
	return ipv4.NewPacketConn(c), nil
}

var _ batchConn = &skipAddrBatchConn{}

// newSkipAddrBatchConn wraps an OOBCapablePacketConn's raw fd.
func newSkipAddrBatchConn(c OOBCapablePacketConn) (*skipAddrBatchConn, error) {
	rawConn, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}
	return &skipAddrBatchConn{conn: rawConn}, nil
}

// ReadBatch issues one recvmmsg and fills N / NN / Flags for ms[:n].
// ms[i].Addr is deliberately left nil.
func (c *skipAddrBatchConn) ReadBatch(ms []ipv4.Message, _ int) (int, error) {
	n := len(ms)
	if n == 0 || n > skipAddrBatchSize {
		return 0, nil
	}
	if c.hs == nil {
		c.hs = make([]mmsghdrSkip, skipAddrBatchSize)
		c.names = make([]byte, skipAddrBatchSize*sizeofSockaddrInet6Skip)
		c.vecs = make([]syscall.Iovec, skipAddrBatchSize)
	}
	for i := 0; i < n; i++ {
		if len(ms[i].Buffers) == 0 || len(ms[i].Buffers[0]) == 0 || len(ms[i].OOB) == 0 {
			return 0, errors.New("skipAddrBatchConn: message without buffer or OOB space")
		}
		name := c.names[i*sizeofSockaddrInet6Skip : (i+1)*sizeofSockaddrInet6Skip : (i+1)*sizeofSockaddrInet6Skip]
		c.vecs[i] = syscall.Iovec{Base: &ms[i].Buffers[0][0], Len: msghdrLen(len(ms[i].Buffers[0]))}
		hdr := syscall.Msghdr{
			Name:    (*byte)(unsafe.Pointer(&name[0])),
			Namelen: uint32(sizeofSockaddrInet6Skip),
			Iov:     &c.vecs[i],
			Iovlen:  1,
			Control: &ms[i].OOB[0],
		}
		// Controllen / Iovlen field widths differ between 64-bit and
		// 32-bit platforms; assign through the native field types so the
		// code compiles on both (the values always fit).
		hdr.Controllen = msghdrLen(len(ms[i].OOB))
		c.hs[i].Hdr = hdr
		c.hs[i].Len = 0
	}
	c.n = n

	c.got = 0
	c.operr = nil
	err := c.conn.Read(c.readCallback)
	if err != nil {
		return 0, err
	}
	if c.operr != nil && c.operr != syscall.EINTR {
		return 0, c.operr
	}
	for i := 0; i < c.got; i++ {
		ms[i].N = int(c.hs[i].Len)
		ms[i].NN = int(c.hs[i].Hdr.Controllen)
		ms[i].Flags = int(c.hs[i].Hdr.Flags)
	}
	return c.got, nil
}

// readCallback is a zero-capture callback stored on the struct: a closure
// capturing locals would heap-allocate itself plus the captured variables
// on every ReadBatch call (3 allocs). All state lives in struct fields.
func (c *skipAddrBatchConn) readCallback(fd uintptr) bool {
	c.got, c.operr = recvmmsgSkip(fd, c.hs[:c.n])
	// Mirrors x/net's ioComplete for flags=0: EAGAIN re-arms the poller
	// and retries; EINTR retries in place; anything else (including
	// success) stops the loop.
	if c.operr == syscall.EAGAIN || c.operr == syscall.EWOULDBLOCK {
		return false
	}
	return true
}

// recvmmsgSkip invokes the recvmmsg syscall with no flags and no timeout.
func recvmmsgSkip(fd uintptr, hs []mmsghdrSkip) (int, error) {
	n, _, errno := syscall.Syscall6(
		unix.SYS_RECVMMSG,
		fd,
		uintptr(unsafe.Pointer(&hs[0])),
		uintptr(len(hs)),
		0,
		0,
		0,
	)
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}
