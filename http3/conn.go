package http3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olicesx/quic-go"
	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/quicvarint"

	"github.com/olicesx/qpack"
)

// Connection is an HTTP/3 connection.
// It has all methods from the quic.Connection expect for AcceptStream, AcceptUniStream,
// SendDatagram and ReceiveDatagram.
type Connection interface {
	OpenStream() (quic.Stream, error)
	OpenStreamSync(context.Context) (quic.Stream, error)
	OpenUniStream() (quic.SendStream, error)
	OpenUniStreamSync(context.Context) (quic.SendStream, error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	CloseWithError(quic.ApplicationErrorCode, string) error
	Context() context.Context
	ConnectionState() quic.ConnectionState

	// ReceivedSettings returns a channel that is closed once the client's SETTINGS frame was received.
	ReceivedSettings() <-chan struct{}
	// Settings returns the settings received on this connection.
	Settings() *Settings
}

// invalidStreamID is a stream ID that is invalid. The first valid stream ID in QUIC is 0.
const invalidStreamID = quic.StreamID(-1)

type connection struct {
	quic.Connection
	ctx context.Context

	perspective protocol.Perspective
	logger      *slog.Logger

	enableDatagrams bool

	decoder *qpack.Decoder

	streamMx sync.Mutex
	streams  map[protocol.StreamID]*datagrammer
	// openingRequests counts request stream opens that are in flight but have
	// not registered a stream yet. It's protected by streamMx. A GOAWAY that
	// arrives in that window must not close the connection out from under the
	// request, so the open counts as activity until it resolves.
	openingRequests int

	settings         *Settings
	receivedSettings chan struct{}

	// maxStreamID is set once a GOAWAY frame is received (client only).
	// It's protected by streamMx.
	maxStreamID quic.StreamID
	// goAwayCtx is cancelled once a GOAWAY frame is received (client only).
	goAwayCtx    context.Context
	goAwayCancel context.CancelFunc

	// controlStrHandler is called *after* the SETTINGS frame was parsed.
	// The client uses it to keep reading the control stream (GOAWAY frames).
	controlStrHandler func(quic.ReceiveStream, *frameParser)
	// onStreamsEmpty is called when the last request stream was closed.
	onStreamsEmpty func()

	idleTimeout time.Duration
	idleTimer   *time.Timer
}

func newConnection(
	ctx context.Context,
	quicConn quic.Connection,
	enableDatagrams bool,
	perspective protocol.Perspective,
	logger *slog.Logger,
	idleTimeout time.Duration,
) *connection {
	c := &connection{
		ctx:              ctx,
		Connection:       quicConn,
		perspective:      perspective,
		logger:           logger,
		idleTimeout:      idleTimeout,
		enableDatagrams:  enableDatagrams,
		decoder:          qpack.NewDecoder(),
		receivedSettings: make(chan struct{}),
		streams:          make(map[protocol.StreamID]*datagrammer),
		maxStreamID:      invalidStreamID,
	}
	c.goAwayCtx, c.goAwayCancel = context.WithCancel(context.Background())
	if idleTimeout > 0 {
		c.idleTimer = time.AfterFunc(idleTimeout, c.onIdleTimer)
	}
	return c
}

func (c *connection) onIdleTimer() {
	c.CloseWithError(quic.ApplicationErrorCode(ErrCodeNoError), "idle timeout")
}

func (c *connection) hasActiveStreams() bool {
	c.streamMx.Lock()
	defer c.streamMx.Unlock()
	return len(c.streams) > 0 || c.openingRequests > 0
}

// beginOpen marks one request stream open as in flight and reports whether the
// open may proceed. A GOAWAY that arrives before the stream is registered must
// not close the connection out from under the request, so an in-flight open
// counts as activity until it resolves; a GOAWAY that arrived first refuses the
// open. The GOAWAY handler stores maxStreamID under streamMx, so checking it in
// the same critical section as the increment means a GOAWAY can neither be
// missed nor slip between the check and the count. beginOpen must be paired
// with either a registration (which transfers the activity to the stream) or
// endOpen.
func (c *connection) beginOpen() bool {
	c.streamMx.Lock()
	defer c.streamMx.Unlock()
	if c.maxStreamID != invalidStreamID {
		return false
	}
	c.openingRequests++
	return true
}

// endOpen releases an in-flight open that did not register a stream. A GOAWAY
// that arrived while it was in flight could not decide to close the connection
// yet, so onStreamsEmpty is asked to look again; that callback owns the
// decision and validates the state itself. This must not run while holding
// streamMx: onStreamsEmpty takes the same lock.
func (c *connection) endOpen() {
	c.streamMx.Lock()
	c.openingRequests--
	c.streamMx.Unlock()

	if c.onStreamsEmpty != nil {
		c.onStreamsEmpty()
	}
}

func (c *connection) clearStream(id quic.StreamID) {
	c.streamMx.Lock()
	delete(c.streams, id)
	if c.idleTimeout > 0 && len(c.streams) == 0 {
		c.idleTimer.Reset(c.idleTimeout)
	}
	c.streamMx.Unlock()

	// The client closes the connection once all request streams are done after
	// it received a GOAWAY frame. onStreamsEmpty owns that decision and
	// re-checks it under streamMx: a snapshot taken here can already be stale
	// (a new stream may have registered, or an open may still be in flight).
	// This must not run while holding streamMx: the handler takes the same lock.
	if c.onStreamsEmpty != nil {
		c.onStreamsEmpty()
	}
}

func (c *connection) openRequestStream(
	ctx context.Context,
	requestWriter *requestWriter,
	reqDone chan<- struct{},
	disableCompression bool,
	maxHeaderBytes uint64,
) (*requestStream, error) {
	// RFC 9114 Section 5.2 prohibits opening any new request streams after GOAWAY.
	// The stream ID only identifies requests that were already in flight and might still be processed.
	// beginOpen checks the GOAWAY state and counts the open in one step.
	if !c.beginOpen() {
		return nil, errGoAway
	}
	// Release the open on every path, including a panic, so the count can never
	// leak and keep deferring the GOAWAY close forever.
	registered := false
	defer func() {
		if !registered {
			c.endOpen()
		}
	}()
	openCtx, cancel := context.WithCancelCause(ctx)
	// A request blocked in OpenStreamSync has no request stream yet, so it is not
	// in flight: GOAWAY cancels the open instead of letting it proceed. It still
	// counts as activity, so the connection is not closed underneath it.
	stop := context.AfterFunc(c.goAwayCtx, func() { cancel(errGoAway) })
	str, err := c.OpenStreamSync(openCtx)
	stop()
	cancel(nil)
	if err != nil {
		if context.Cause(openCtx) == errGoAway {
			return nil, errGoAway
		}
		return nil, err
	}

	// Check again in case GOAWAY raced with OpenStreamSync.
	if c.goAwayCtx.Err() != nil {
		str.CancelRead(quic.StreamErrorCode(ErrCodeRequestCanceled))
		str.CancelWrite(quic.StreamErrorCode(ErrCodeRequestCanceled))
		return nil, errGoAway
	}

	datagrams := newDatagrammer(func(b []byte) error { return c.sendDatagram(str.StreamID(), b) })
	c.streamMx.Lock()
	c.streams[str.StreamID()] = datagrams
	// The registered stream now carries the activity: release the open count in
	// the same critical section, so the connection is never seen as idle in
	// between, and disarm the deferred release.
	c.openingRequests--
	registered = true
	c.streamMx.Unlock()
	qstr := newStateTrackingStream(str, c, datagrams)
	rsp := &http.Response{}
	hstr := newStream(qstr, c, datagrams, func(r io.Reader, l uint64) error {
		hdr, err := c.decodeTrailers(r, l, maxHeaderBytes)
		if err != nil {
			return err
		}
		rsp.Trailer = hdr
		return nil
	})
	trace := httptrace.ContextClientTrace(ctx)
	return newRequestStream(hstr, requestWriter, reqDone, c.decoder, disableCompression, maxHeaderBytes, rsp, trace), nil
}

func (c *connection) decodeTrailers(r io.Reader, l, maxHeaderBytes uint64) (http.Header, error) {
	if l > maxHeaderBytes {
		return nil, fmt.Errorf("HEADERS frame too large: %d bytes (max: %d)", l, maxHeaderBytes)
	}

	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return parseTrailersIncremental(c.decoder.Decode(b), int(maxHeaderBytes))
}

func (c *connection) acceptStream(ctx context.Context) (quic.Stream, *datagrammer, error) {
	str, err := c.AcceptStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	datagrams := newDatagrammer(func(b []byte) error { return c.sendDatagram(str.StreamID(), b) })
	if c.perspective == protocol.PerspectiveServer {
		strID := str.StreamID()
		c.streamMx.Lock()
		c.streams[strID] = datagrams
		if c.idleTimeout > 0 {
			if len(c.streams) == 1 {
				c.idleTimer.Stop()
			}
		}
		c.streamMx.Unlock()
		str = newStateTrackingStream(str, c, datagrams)
	}
	return str, datagrams, nil
}

func (c *connection) CloseWithError(code quic.ApplicationErrorCode, msg string) error {
	if c.idleTimer != nil {
		c.idleTimer.Stop()
	}
	return c.Connection.CloseWithError(code, msg)
}

func (c *connection) handleUnidirectionalStreams(hijack func(StreamType, quic.ConnectionTracingID, quic.ReceiveStream, error) (hijacked bool)) {
	var (
		rcvdControlStr      atomic.Bool
		rcvdQPACKEncoderStr atomic.Bool
		rcvdQPACKDecoderStr atomic.Bool
	)

	for {
		str, err := c.AcceptUniStream(context.Background())
		if err != nil {
			if c.logger != nil {
				c.logger.Debug("accepting unidirectional stream failed", "error", err)
			}
			return
		}

		go func(str quic.ReceiveStream) {
			streamType, err := quicvarint.Read(quicvarint.NewReader(str))
			if err != nil {
				id := c.Connection.Context().Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID)
				if hijack != nil && hijack(StreamType(streamType), id, str, err) {
					return
				}
				if c.logger != nil {
					c.logger.Debug("reading stream type on stream failed", "stream ID", str.StreamID(), "error", err)
				}
				return
			}
			// We're only interested in the control stream here.
			switch streamType {
			case streamTypeControlStream:
			case streamTypeQPACKEncoderStream:
				if isFirst := rcvdQPACKEncoderStr.CompareAndSwap(false, true); !isFirst {
					c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeStreamCreationError), "duplicate QPACK encoder stream")
				}
				// Our QPACK implementation doesn't use the dynamic table yet.
				return
			case streamTypeQPACKDecoderStream:
				if isFirst := rcvdQPACKDecoderStr.CompareAndSwap(false, true); !isFirst {
					c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeStreamCreationError), "duplicate QPACK decoder stream")
				}
				// Our QPACK implementation doesn't use the dynamic table yet.
				return
			case streamTypePushStream:
				switch c.perspective {
				case protocol.PerspectiveClient:
					// we never increased the Push ID, so we don't expect any push streams
					c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeIDError), "")
				case protocol.PerspectiveServer:
					// only the server can push
					c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeStreamCreationError), "")
				}
				return
			default:
				if hijack != nil {
					if hijack(
						StreamType(streamType),
						c.Connection.Context().Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID),
						str,
						nil,
					) {
						return
					}
				}
				str.CancelRead(quic.StreamErrorCode(ErrCodeStreamCreationError))
				return
			}
			// Only a single control stream is allowed.
			if isFirstControlStr := rcvdControlStr.CompareAndSwap(false, true); !isFirstControlStr {
				c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeStreamCreationError), "duplicate control stream")
				return
			}
			c.handleControlStream(str)
		}(str)
	}
}

// handleControlStream handles the control stream, after the stream type was read.
// It parses the SETTINGS frame and sets up HTTP Datagrams, if supported.
func (c *connection) handleControlStream(str quic.ReceiveStream) {
	// The control stream is unidirectional: the frame parser needs to know
	// that it must not treat a PUSH_PROMISE frame as a protocol violation of
	// type H3_ID_ERROR (that's reserved for request streams).
	fp := &frameParser{closeConn: c.CloseWithError, r: str, streamID: str.StreamID()}
	f, err := fp.ParseNext()
	if err != nil {
		c.CloseWithError(quic.ApplicationErrorCode(ErrCodeFrameError), "")
		return
	}
	sf, ok := f.(*settingsFrame)
	if !ok {
		c.CloseWithError(quic.ApplicationErrorCode(ErrCodeMissingSettings), "")
		return
	}
	c.settings = &Settings{
		EnableDatagrams:       sf.Datagram,
		EnableExtendedConnect: sf.ExtendedConnect,
		Other:                 sf.Other,
	}
	close(c.receivedSettings)
	if sf.Datagram {
		// If datagram support was enabled on our side as well as on the server side,
		// we can expect it to have been negotiated both on the transport and on the HTTP/3 layer.
		// Note: ConnectionState() will block until the handshake is complete (relevant when using 0-RTT).
		if c.enableDatagrams && !c.Connection.ConnectionState().SupportsDatagrams {
			c.CloseWithError(quic.ApplicationErrorCode(ErrCodeSettingsError), "missing QUIC Datagram support")
			return
		}
		go func() {
			if err := c.receiveDatagrams(); err != nil {
				if c.logger != nil {
					c.logger.Debug("receiving datagrams failed", "error", err)
				}
			}
		}()
	}

	if c.controlStrHandler != nil {
		c.controlStrHandler(str, fp)
	}
}

func (c *connection) sendDatagram(streamID protocol.StreamID, b []byte) error {
	// TODO: this creates a lot of garbage and an additional copy
	data := make([]byte, 0, len(b)+8)
	data = quicvarint.Append(data, uint64(streamID/4))
	data = append(data, b...)
	return c.SendDatagram(data)
}

func (c *connection) receiveDatagrams() error {
	for {
		b, err := c.ReceiveDatagram(context.Background())
		if err != nil {
			return err
		}
		quarterStreamID, n, err := quicvarint.Parse(b)
		if err != nil {
			c.ReleaseDatagram(b)
			c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeDatagramError), "")
			return fmt.Errorf("could not read quarter stream id: %w", err)
		}
		if quarterStreamID > maxQuarterStreamID {
			c.ReleaseDatagram(b)
			c.Connection.CloseWithError(quic.ApplicationErrorCode(ErrCodeDatagramError), "")
			return fmt.Errorf("invalid quarter stream id: %d", quarterStreamID)
		}
		streamID := protocol.StreamID(4 * quarterStreamID)
		c.streamMx.Lock()
		dg, ok := c.streams[streamID]
		c.streamMx.Unlock()
		if !ok {
			c.ReleaseDatagram(b)
			continue
		}
		payload := append([]byte(nil), b[n:]...)
		c.ReleaseDatagram(b)
		dg.enqueue(payload)
	}
}

// ReceivedSettings returns a channel that is closed once the peer's SETTINGS frame was received.
// Settings can be optained from the Settings method after the channel was closed.
func (c *connection) ReceivedSettings() <-chan struct{} { return c.receivedSettings }

// Settings returns the settings received on this connection.
// It is only valid to call this function after the channel returned by ReceivedSettings was closed.
func (c *connection) Settings() *Settings { return c.settings }

// Context returns the context of the underlying QUIC connection.
func (c *connection) Context() context.Context { return c.ctx }
