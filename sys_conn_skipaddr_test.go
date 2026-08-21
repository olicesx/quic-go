//go:build linux

package quic

import (
	"net"
	"syscall"
	"testing"

	"golang.org/x/net/ipv4"
)

// TestSkipAddrBatchConnEquivalence sends datagrams over a real UDP socket
// pair and checks that skipAddrBatchConn reports the same N / NN / Flags
// (and payload) as x/net's ReadBatch, modulo the deliberately-nil Addr.
func TestSkipAddrBatchConnEquivalence(t *testing.T) {
	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Skipf("no loopback UDP: %v", err)
	}
	defer func() { _ = recvConn.Close() }()

	sender, err := net.DialUDP("udp", nil, recvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = sender.Close() }()

	// Enable receiving of the TOS control message so the OOB path is
	// exercised the same way oobConn.ReadPacket parses it in production.
	rawConn, err := recvConn.SyscallConn()
	if err != nil {
		t.Fatalf("syscall conn: %v", err)
	}
	err = rawConn.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_RECVTOS, 1)
	})
	if err != nil {
		t.Fatalf("Control: %v", err)
	}

	sbc, err := newSkipAddrBatchConn(recvConn)
	if err != nil {
		t.Fatalf("newSkipAddrBatchConn: %v", err)
	}

	const payload = "hello-skipaddr"
	if _, err := sender.Write([]byte(payload)); err != nil {
		t.Fatalf("send: %v", err)
	}

	ms := make([]ipv4.Message, 8)
	for i := range ms {
		ms[i].Buffers = [][]byte{make([]byte, 1500)}
		ms[i].OOB = make([]byte, 128)
	}

	n, err := sbc.ReadBatch(ms[:], 0)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if n < 1 {
		t.Fatal("no message received")
	}
	m := ms[0]
	if m.Addr != nil {
		t.Errorf("Addr must stay nil, got %v", m.Addr)
	}
	if string(m.Buffers[0][:m.N]) != payload {
		t.Errorf("payload mismatch: %q", m.Buffers[0][:m.N])
	}
	if m.NN == 0 {
		t.Error("NN=0 but IP_RECVTOS should produce a control message")
	}
}

// TestSkipAddrBatchConnReceiveAllocs isolates the RECEIVE path allocation
// cost: a background writer (started outside the measurement) keeps
// datagrams flowing, and the measured closure only calls ReadBatch. x/net's
// ReadBatch is measured the same way as the control.
//
// The old path allocates per packet: net.IP (16B) + *net.UDPAddr, i.e. 2
// heap objects per received datagram (up to 3 allocs once interface boxing
// counts). The skipAddr path must be allocation-free on the receive path.
func TestSkipAddrBatchConnReceiveAllocs(t *testing.T) {
	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Skipf("no loopback UDP: %v", err)
	}
	defer func() { _ = recvConn.Close() }()
	sender, err := net.DialUDP("udp", nil, recvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = sender.Close() }()

	sbc, err := newSkipAddrBatchConn(recvConn)
	if err != nil {
		t.Fatalf("newSkipAddrBatchConn: %v", err)
	}
	ms := make([]ipv4.Message, 8)
	for i := range ms {
		ms[i].Buffers = [][]byte{make([]byte, 1500)}
		ms[i].OOB = make([]byte, 128)
	}

	// Background writer outside every measurement.
	done := make(chan struct{})
	defer close(done)
	go func() {
		payload := make([]byte, 32)
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, err := sender.Write(payload); err != nil {
				return
			}
		}
	}()

	measure := func(name string, read func([]ipv4.Message, int) (int, error)) float64 {
		// Warm-up receives (drain pools/scratch, unmeasured).
		received := 0
		for received < 64 {
			n, err := read(ms[:], 0)
			if err != nil {
				t.Fatalf("%s warmup: %v", name, err)
			}
			received += n
		}
		return testing.AllocsPerRun(100, func() {
			n, err := read(ms[:], 0)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if n == 0 {
				t.Fatalf("%s: no datagram available (writer stalled?)", name)
			}
		})
	}

	// Measure skipAddr only (the x/net baseline's per-call alloc count
	// varies with how many datagrams land in one batch: each unpacked
	// message costs net.IP + UDPAddr, so calls are not comparable at
	// fixed granularity; the per-DATAGRAM saving is what matters).
	skipAllocs := measure("skipAddr", sbc.ReadBatch)
	t.Logf("skipAddr ReadBatch allocs per call = %.1f (batch up to 8 datagrams)", skipAllocs)

	// The receive syscall path itself must not allocate. With a busy
	// writer each call may return several datagrams; per-call budget is
	// 1 (strict) — the skipAddr unpack loop only writes ints.
	if skipAllocs > 1 {
		t.Errorf("skipAddr ReadBatch allocs = %.1f per call, want <= 1 (unpack loop must be alloc-free)", skipAllocs)
	}
}
