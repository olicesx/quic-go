package wire

import (
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
)

// BenchmarkParseDatagramFramePooled measures the real wire-level parse path:
// pooled frame + pooled Data buffer, returned to the pool afterwards.
// This is what connection.handleDatagramFrame does per incoming datagram.
func BenchmarkParseDatagramFramePooled(b *testing.B) {
	payload := make([]byte, 1200)
	f := &DatagramFrame{DataLenPresent: true, Data: payload}
	wireData, _ := f.Append(nil, protocol.Version1)
	typ := uint64(0x31) // DATAGRAM with length

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// parseDatagramFrame receives the payload after the frame type byte
		got, _, err := parseDatagramFrame(wireData[1:], typ, protocol.Version1)
		if err != nil {
			b.Fatal(err)
		}
		if len(got.Data) != 1200 {
			b.Fatal("bad parse")
		}
		PutDatagramFrame(got)
	}
}

// BenchmarkParseDatagramFrameLegacy is the pre-pool baseline: fresh frame and
// fresh Data per datagram (what the original parseDatagramFrame did).
func BenchmarkParseDatagramFrameLegacy(b *testing.B) {
	payload := make([]byte, 1200)
	f := &DatagramFrame{DataLenPresent: true, Data: payload}
	wireData, _ := f.Append(nil, protocol.Version1)
	typ := uint64(0x31)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// legacy: fresh frame + fresh buffer (parse receives post-type payload)
		f2 := &DatagramFrame{DataLenPresent: typ&0x1 > 0}
		f2.Data = make([]byte, 1200)
		copy(f2.Data, wireData[len(wireData)-1200:])
		if len(f2.Data) != 1200 {
			b.Fatal("bad parse")
		}
	}
}
