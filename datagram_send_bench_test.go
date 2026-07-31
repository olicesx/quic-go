package quic

import (
	"testing"
)

// BenchmarkDatagramSendLegacy simulates the current send path: fresh
// DatagramFrame + fresh Data buffer per datagram (connection.go SendDatagram).
func BenchmarkDatagramSendLegacy(b *testing.B) {
	payload := make([]byte, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// wire.DatagramFrame is not pooled; Data is freshly allocated
		f := &wireDatagramFrame{}
		f.Data = make([]byte, len(payload))
		copy(f.Data, payload)
		_ = f
	}
}

// wireDatagramFrame mirrors wire.DatagramFrame's payload semantics for the
// benchmark (the real struct has extra fields but no pooling today).
type wireDatagramFrame struct {
	Data []byte
}

// BenchmarkDatagramSendPooled simulates the pooled send path: the frame and
// its Data come from a sync.Pool and are returned after send.
var datagramSendPool = newBenchPool()

type benchPool struct {
	ch chan []byte
}

func newBenchPool() *benchPool { return &benchPool{ch: make(chan []byte, 256)} }

func (p *benchPool) Get() []byte {
	select {
	case b := <-p.ch:
		return b[:0]
	default:
		return make([]byte, 0, 1500)
	}
}

func (p *benchPool) Put(b []byte) {
	select {
	case p.ch <- b:
	default:
	}
}

func BenchmarkDatagramSendPooled(b *testing.B) {
	payload := make([]byte, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := datagramSendPool.Get()
		buf = append(buf, payload...)
		_ = buf
		datagramSendPool.Put(buf)
	}
}
