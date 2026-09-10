package quic

import (
	"errors"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/qerr"
	"github.com/olicesx/quic-go/internal/utils"
	"github.com/olicesx/quic-go/logging"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// levelRecorder is a utils.Logger that only records the level every call was
// made at. It lets a test pin down which level a teardown is reported at,
// independently of the log level the process runs with.
type levelRecorder struct {
	levels []string
}

var _ utils.Logger = &levelRecorder{}

func (l *levelRecorder) SetLogLevel(utils.LogLevel)     {}
func (l *levelRecorder) SetLogTimeFormat(string)        {}
func (l *levelRecorder) Debug() bool                    { return false }
func (l *levelRecorder) WithPrefix(string) utils.Logger { return l }
func (l *levelRecorder) Errorf(string, ...interface{})  { l.levels = append(l.levels, "error") }
func (l *levelRecorder) Infof(string, ...interface{})   { l.levels = append(l.levels, "info") }
func (l *levelRecorder) Debugf(string, ...interface{})  { l.levels = append(l.levels, "debug") }

func recordedLevels(t *testing.T, teardown func(s *connection)) []string {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	conn := newClientTestConnection(t, mockCtrl, &Config{DisablePathMTUDiscovery: true}, false)
	rec := &levelRecorder{}
	conn.conn.logger = rec
	teardown(conn.conn)
	return rec.levels
}

// A routine teardown carries no error, so it must not be reported at error
// level: dae closes every DNS-over-QUIC connection with CloseWithError(0, ""),
// and that used to put one error line on every such close.
func TestTeardownLogLevels(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		levels []string
	}{
		{"closeLocal without a cause", nil, []string{"info"}},
		{"closeLocal with application NO_ERROR", &qerr.ApplicationError{ErrorCode: 0, ErrorMessage: "done"}, []string{"info"}},
		{"closeLocal with an application error", &qerr.ApplicationError{ErrorCode: 42}, []string{"error"}},
		{"closeLocal with a transport error", &qerr.TransportError{ErrorCode: qerr.InternalError}, []string{"error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.levels, recordedLevels(t, func(s *connection) { s.closeLocal(tc.err) }))
		})
	}

	for _, tc := range []struct {
		name   string
		err    error
		levels []string
	}{
		{"closeRemote without a cause", nil, []string{"info"}},
		{"closeRemote with application NO_ERROR", &qerr.ApplicationError{Remote: true, ErrorCode: 0}, []string{"info"}},
		{"closeRemote with an application error", &qerr.ApplicationError{Remote: true, ErrorCode: 1}, []string{"error"}},
		{"closeRemote with a transport error", &qerr.TransportError{Remote: true, ErrorCode: qerr.ProtocolViolation}, []string{"error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.levels, recordedLevels(t, func(s *connection) { s.closeRemote(tc.err) }))
		})
	}

	for _, tc := range []struct {
		name   string
		err    error
		levels []string
	}{
		{"destroyImpl without a cause", nil, []string{"info"}},
		{"destroyImpl on idle timeout", qerr.ErrIdleTimeout, []string{"info"}},
		{"destroyImpl on handshake timeout", qerr.ErrHandshakeTimeout, []string{"error"}},
		{"destroyImpl on a transport error", &qerr.TransportError{ErrorCode: qerr.InternalError}, []string{"error"}},
		{"destroyImpl on an arbitrary error", errors.New("boom"), []string{"error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.levels, recordedLevels(t, func(s *connection) { s.destroyImpl(tc.err) }))
		})
	}
}

// The undecryptable-packet queue is touched once per packet, so its two lines
// must stay at debug level, the level that carries per-packet detail.
func TestUndecryptablePacketLogLevel(t *testing.T) {
	t.Run("queueing", func(t *testing.T) {
		levels := recordedLevels(t, func(s *connection) {
			s.tryQueueingUndecryptablePacket(receivedPacket{buffer: getPacketBuffer(), data: make([]byte, 1200)}, logging.PacketTypeHandshake)
		})
		require.Equal(t, []string{"debug"}, levels)
	})

	t.Run("queue full", func(t *testing.T) {
		levels := recordedLevels(t, func(s *connection) {
			for i := 0; i < protocol.MaxUndecryptablePackets; i++ {
				s.undecryptablePackets = append(s.undecryptablePackets, receivedPacket{buffer: getPacketBuffer(), data: make([]byte, 1200)})
			}
			s.tryQueueingUndecryptablePacket(receivedPacket{buffer: getPacketBuffer(), data: make([]byte, 1200)}, logging.PacketTypeHandshake)
		})
		require.Equal(t, []string{"debug"}, levels)
	})
}
