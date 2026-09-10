package http3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/olicesx/quic-go"
	mockquic "github.com/olicesx/quic-go/internal/mocks/quic"
	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/quicvarint"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// goAwayTestConn wires up a ClientConn on top of a mock QUIC connection.
// The peer's control stream is fed through an io.Pipe, so frames (GOAWAY,
// PUSH_PROMISE, ...) can be sent after the connection was set up.
type goAwayTestConn struct {
	mockCtrl    *gomock.Controller
	cc          *ClientConn
	conn        *mockquic.MockEarlyConnection
	controlPipe *io.PipeWriter
	done        chan struct{}
}

func newGoAwayTestConn(t *testing.T) *goAwayTestConn {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	conn := mockquic.NewMockEarlyConnection(mockCtrl)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })

	conn.EXPECT().Context().Return(context.Background()).AnyTimes()
	conn.EXPECT().HandshakeComplete().Return(closedChan()).AnyTimes()

	// the client's own control stream, used to send the SETTINGS frame
	settingsWritten := make(chan struct{})
	clientControlStr := mockquic.NewMockStream(mockCtrl)
	clientControlStr.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
		select {
		case <-settingsWritten:
		default:
			close(settingsWritten)
		}
		return len(b), nil
	}).AnyTimes()
	conn.EXPECT().OpenUniStream().Return(clientControlStr, nil).AnyTimes()

	// the server's control stream
	pr, pw := io.Pipe()
	controlStr := mockquic.NewMockStream(mockCtrl)
	controlStr.EXPECT().Read(gomock.Any()).DoAndReturn(pr.Read).AnyTimes()
	controlStr.EXPECT().StreamID().Return(quic.StreamID(3)).AnyTimes()
	conn.EXPECT().AcceptUniStream(gomock.Any()).Return(controlStr, nil)
	conn.EXPECT().AcceptUniStream(gomock.Any()).DoAndReturn(func(context.Context) (quic.ReceiveStream, error) {
		<-done
		return nil, errors.New("test done")
	})

	cc := (&Transport{}).NewClientConn(conn)
	// wait for the client's SETTINGS frame, to make sure the connection is set up
	select {
	case <-settingsWritten:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the client's SETTINGS frame")
	}
	_, err := pw.Write((&settingsFrame{}).Append(quicvarint.Append(nil, streamTypeControlStream)))
	require.NoError(t, err)
	select {
	case <-cc.ReceivedSettings():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the SETTINGS frame")
	}
	return &goAwayTestConn{mockCtrl: mockCtrl, cc: cc, conn: conn, controlPipe: pw, done: done}
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (c *goAwayTestConn) sendFrame(t *testing.T, b []byte) {
	t.Helper()
	_, err := c.controlPipe.Write(b)
	require.NoError(t, err)
}

// expectClose asserts that the connection is closed with the given error code.
func (c *goAwayTestConn) expectClose() <-chan quic.ApplicationErrorCode {
	closed := make(chan quic.ApplicationErrorCode, 1)
	c.conn.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).DoAndReturn(
		func(code quic.ApplicationErrorCode, _ string) error {
			select {
			case closed <- code:
			default:
			}
			return nil
		},
	).AnyTimes()
	return closed
}

// openRequestStream opens a request stream, and returns the underlying QUIC
// stream mock. The caller is responsible for closing it.
func (c *goAwayTestConn) openRequestStream(t *testing.T, id quic.StreamID) *mockquic.MockStream {
	t.Helper()
	str := mockquic.NewMockStream(c.mockCtrl)
	str.EXPECT().StreamID().Return(id).AnyTimes()
	str.EXPECT().Context().Return(context.Background()).AnyTimes()
	str.EXPECT().Close().Return(nil).AnyTimes()
	str.EXPECT().CancelRead(gomock.Any()).AnyTimes()
	str.EXPECT().CancelWrite(gomock.Any()).AnyTimes()
	str.EXPECT().Write(gomock.Any()).Return(0, nil).AnyTimes()
	c.conn.EXPECT().OpenStreamSync(gomock.Any()).Return(str, nil)
	return str
}

// A GOAWAY frame received on the control stream must be visible to the client:
// no new request streams may be opened, and the connection is closed with
// H3_NO_ERROR once the last request stream is done (RFC 9114, Section 5.2).
func TestClientConnGoAwayNoActiveStreams(t *testing.T) {
	tc := newGoAwayTestConn(t)
	closed := tc.expectClose()

	tc.sendFrame(t, (&goAwayFrame{StreamID: 8}).Append(nil))

	select {
	case code := <-closed:
		require.Equal(t, quic.ApplicationErrorCode(ErrCodeNoError), code)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the connection to be closed")
	}

	// GOAWAY is visible: the client doesn't open new request streams.
	_, err := tc.cc.OpenRequestStream(context.Background())
	require.ErrorIs(t, err, errGoAway)
}

func TestClientConnGoAwayWithActiveStream(t *testing.T) {
	tc := newGoAwayTestConn(t)
	qstr := tc.openRequestStream(t, 0)
	str, err := tc.cc.OpenRequestStream(context.Background())
	require.NoError(t, err)
	closed := tc.expectClose()

	tc.sendFrame(t, (&goAwayFrame{StreamID: 8}).Append(nil))

	// The connection is not closed while the request is still in flight:
	// GOAWAY allows in-flight requests to finish.
	select {
	case code := <-closed:
		t.Fatalf("connection closed while a request is in flight: %d", code)
	case <-time.After(scaleDuration(10 * time.Millisecond)):
	}

	// GOAWAY is visible: no new request streams...
	_, err = tc.cc.OpenRequestStream(context.Background())
	require.ErrorIs(t, err, errGoAway)
	// ... and no stream was opened and immediately cancelled behind the caller's back.
	require.Equal(t, quic.StreamID(0), qstr.StreamID())

	// Once the request is done, the connection is closed with H3_NO_ERROR.
	require.NoError(t, str.Close())
	str.CancelRead(1337)

	select {
	case code := <-closed:
		require.Equal(t, quic.ApplicationErrorCode(ErrCodeNoError), code)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the connection to be closed")
	}
}

func TestClientConnGoAwayFailures(t *testing.T) {
	tests := []struct {
		name string
		// frames written to the peer's control stream, after the SETTINGS frame
		frames   func() []byte
		closeErr error
		code     ErrCode
	}{
		{
			name: "not a GOAWAY frame",
			frames: func() []byte {
				return (&headersFrame{Length: 0}).Append(nil)
			},
			code: ErrCodeFrameUnexpected,
		},
		{
			name: "PUSH_PROMISE frame",
			frames: func() []byte {
				return (&pushPromiseFrame{Length: 0}).Append(nil)
			},
			code: ErrCodeFrameUnexpected,
		},
		{
			name: "invalid stream ID",
			frames: func() []byte {
				return (&goAwayFrame{StreamID: 1}).Append(nil)
			},
			code: ErrCodeIDError,
		},
		{
			name: "stream closed before GOAWAY",
			frames: func() []byte {
				return nil
			},
			closeErr: io.EOF,
			code:     ErrCodeClosedCriticalStream,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newGoAwayTestConn(t)
			closed := tc.expectClose()

			if tt.closeErr != nil {
				require.NoError(t, tc.controlPipe.CloseWithError(tt.closeErr))
			} else {
				tc.sendFrame(t, tt.frames())
			}

			select {
			case code := <-closed:
				require.Equal(t, quic.ApplicationErrorCode(tt.code), code)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for the connection to be closed")
			}
		})
	}
}

// The server is not allowed to increase the stream ID in subsequent GOAWAY frames.
func TestClientConnGoAwayIncreasedStreamID(t *testing.T) {
	tc := newGoAwayTestConn(t)
	tc.openRequestStream(t, 0)
	_, err := tc.cc.OpenRequestStream(context.Background())
	require.NoError(t, err)
	closed := tc.expectClose()

	b := (&goAwayFrame{StreamID: 4}).Append(nil)
	b = (&goAwayFrame{StreamID: 8}).Append(b)
	tc.sendFrame(t, b)

	select {
	case code := <-closed:
		require.Equal(t, quic.ApplicationErrorCode(ErrCodeIDError), code)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the connection to be closed")
	}
}

// This endpoint never sends a MAX_PUSH_ID frame, so a PUSH_PROMISE frame
// received on a request stream is a connection error of type H3_ID_ERROR
// (RFC 9114, Section 7.2.5).
func TestStreamRejectsPushPromise(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	qstr := mockquic.NewMockStream(mockCtrl)
	frame := (&pushPromiseFrame{Length: 0}).Append(nil)
	qstr.EXPECT().Read(gomock.Any()).DoAndReturn(bytes.NewReader(frame).Read).AnyTimes()
	conn := mockquic.NewMockEarlyConnection(mockCtrl)
	conn.EXPECT().CloseWithError(quic.ApplicationErrorCode(ErrCodeIDError), gomock.Any())
	str := newStream(
		qstr,
		newConnection(context.Background(), conn, false, protocol.PerspectiveClient, nil, 0),
		nil,
		func(io.Reader, uint64) error { return nil },
	)

	_, err := str.Read(make([]byte, 16))
	require.ErrorContains(t, err, "PUSH_PROMISE")
}
