package wire

import (
	"github.com/olicesx/quic-go/internal/protocol"
)

// A PingFrame is a PING frame
type PingFrame struct{}

// PingFrameSingleton is the shared PING frame instance: the type is
// stateless (Append writes to the output buffer, never to the frame), so
// every allocation site — the parser, the packer, the retransmission queue,
// MTU probes and keepalives — can share one instance instead of allocating
// an empty struct per control event.
var PingFrameSingleton = &PingFrame{}

func (f *PingFrame) Append(b []byte, _ protocol.Version) ([]byte, error) {
	return append(b, pingFrameType), nil
}

// Length of a written frame
func (f *PingFrame) Length(_ protocol.Version) protocol.ByteCount {
	return 1
}
