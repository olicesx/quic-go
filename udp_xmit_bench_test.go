package quic

import (
	"net"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmsghdr mirrors the kernel struct: struct msghdr + unsigned int msg_len,
// padded to 64 bytes on amd64. unix.Msghdr is 56 bytes and lacks msg_len,
// so a []unix.Msghdr passed to SYS_SENDMMSG has the wrong element stride
// and every element after the first is garbage to the kernel.
type mmsghdr struct {
	MsgHdr unix.Msghdr
	MsgLen uint32
	_      uint32 // padding to 64 bytes
}

// BenchmarkUDPXmit quantifies the three candidate send paths for the
// quic-go UDP relay (hy2 datagram) workload:
//
//	single - one sendmsg per QUIC packet (current quic-go behavior:
//	         GSO never fires because datagram packets are not full-size)
//	gso    - N same-size QUIC packets packed into one buffer, sent with
//	         UDP_SEGMENT (Scheme A: relax the ==maxSize merge condition)
//	mmsg   - N independent packets via one sendmmsg syscall (Scheme B)
//
// Each iteration sends ONE packet of data (batch/segment size chosen by the
// benchmark name), so ops/sec is comparable across schemes.
//
// Result files: benchmarks produce pps + throughput. Run with -benchtime=3s.

type udpXmitBench struct {
	fd        int
	segSize   int
	payload   []byte // one packet
	gsoBuf    []byte // segSize*batch for GSO merging
	gsoOob    []byte
	mmsgs     []mmsghdr
	iovs      []unix.Iovec
	keepAlive [][]byte
}

func newUDPXmitBench(b *testing.B, segSize, batch int) *udpXmitBench {
	b.Helper()
	rc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 65536)
		for {
			n, _, err := rc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_ = n
		}
	}()
	b.Cleanup(func() {
		rc.Close()
		<-done
	})

	sc, err := net.DialUDP("udp4", nil, rc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sc.Close() })
	raw, _ := sc.SyscallConn()
	var fd int
	raw.Control(func(f uintptr) { fd = int(f) })

	bench := &udpXmitBench{
		fd:      fd,
		segSize: segSize,
		payload: make([]byte, segSize),
	}
	bench.gsoOob = appendUDPSegmentSizeMsg(make([]byte, 0, 32), uint16(segSize))

	if batch > 0 {
		big := make([]byte, segSize*batch)
		bench.keepAlive = append(bench.keepAlive, big)
		bench.gsoBuf = big
		bench.mmsgs = make([]mmsghdr, batch)
		bench.iovs = make([]unix.Iovec, batch)
		for i := 0; i < batch; i++ {
			bench.iovs[i] = unix.Iovec{Base: &big[i*segSize], Len: uint64(segSize)}
			bench.mmsgs[i].MsgHdr.Iov = &bench.iovs[i]
			bench.mmsgs[i].MsgHdr.Iovlen = 1
		}
	}
	return bench
}

func (x *udpXmitBench) sendSingle() error {
	return unix.Sendmsg(x.fd, x.payload, nil, nil, 0)
}

func (x *udpXmitBench) sendGSO(batch int) error {
	return unix.Sendmsg(x.fd, x.gsoBuf[:x.segSize*batch], x.gsoOob, nil, 0)
}

func (x *udpXmitBench) sendMMsg(batch int) error {
	_, _, errno := unix.Syscall6(unix.SYS_SENDMMSG, uintptr(x.fd),
		uintptr(unsafe.Pointer(&x.mmsgs[0])), uintptr(batch), 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func BenchmarkUDPXmit(b *testing.B) {
	scenarios := []struct {
		name  string
		seg   int
		batch int
		op    func(*udpXmitBench, int) error
	}{
		{"single/1230", 1230, 0, func(x *udpXmitBench, _ int) error { return x.sendSingle() }},
		{"single/100", 100, 0, func(x *udpXmitBench, _ int) error { return x.sendSingle() }},
		{"gso8/1230", 1230, 8, (*udpXmitBench).sendGSO},
		{"gso32/1230", 1230, 32, (*udpXmitBench).sendGSO},
		{"gso48/1230", 1230, 48, (*udpXmitBench).sendGSO},
		{"gso8/100", 100, 8, (*udpXmitBench).sendGSO},
		{"gso32/100", 100, 32, (*udpXmitBench).sendGSO},
		{"mmsg8/1230", 1230, 8, (*udpXmitBench).sendMMsg},
		{"mmsg32/1230", 1230, 32, (*udpXmitBench).sendMMsg},
		{"mmsg64/1230", 1230, 64, (*udpXmitBench).sendMMsg},
		{"mmsg8/100", 100, 8, (*udpXmitBench).sendMMsg},
		{"mmsg32/100", 100, 32, (*udpXmitBench).sendMMsg},
	}

	for _, s := range scenarios {
		s := s
		b.Run(s.name, func(b *testing.B) {
			x := newUDPXmitBench(b, s.seg, s.batch)
			b.ReportAllocs()
			perOp := s.seg
			if s.batch > 0 {
				perOp = s.seg * s.batch
			}
			b.SetBytes(int64(perOp))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.op(x, s.batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
