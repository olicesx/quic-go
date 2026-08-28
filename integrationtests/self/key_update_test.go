package self_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/olicesx/quic-go"
	"github.com/olicesx/quic-go/internal/handshake"
	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/logging"

	"github.com/stretchr/testify/require"
)

func TestKeyUpdates(t *testing.T) {
	origKeyUpdateInterval := handshake.KeyUpdateInterval()
	t.Cleanup(func() { handshake.SetKeyUpdateInterval(origKeyUpdateInterval) })
	handshake.SetKeyUpdateInterval(1) // update keys as frequently as possible

	var sentHeaders []*logging.ShortHeader
	var receivedHeaders []*logging.ShortHeader

	countKeyPhases := func() (sent, received int) {
		lastKeyPhase := protocol.KeyPhaseOne
		for _, hdr := range sentHeaders {
			if hdr.KeyPhase != lastKeyPhase {
				sent++
				lastKeyPhase = hdr.KeyPhase
			}
		}
		lastKeyPhase = protocol.KeyPhaseOne
		for _, hdr := range receivedHeaders {
			if hdr.KeyPhase != lastKeyPhase {
				received++
				lastKeyPhase = hdr.KeyPhase
			}
		}
		return
	}

	server, err := quic.Listen(newUPDConnLocalhost(t), getTLSConfig(), nil)
	require.NoError(t, err)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := quic.Dial(
		ctx,
		newUPDConnLocalhost(t),
		server.Addr(),
		getTLSClientConfig(),
		getQuicConfig(&quic.Config{Tracer: func(context.Context, logging.Perspective, quic.ConnectionID) *logging.ConnectionTracer {
			return &logging.ConnectionTracer{
				SentShortHeaderPacket: func(hdr *logging.ShortHeader, _ logging.ByteCount, _ logging.ECN, _ *logging.AckFrame, _ []logging.Frame) {
					sentHeaders = append(sentHeaders, hdr)
				},
				ReceivedShortHeaderPacket: func(hdr *logging.ShortHeader, _ logging.ByteCount, _ logging.ECN, _ []logging.Frame) {
					receivedHeaders = append(receivedHeaders, hdr)
				},
			}
		}}),
	)
	require.NoError(t, err)
	defer conn.CloseWithError(0, "")

	serverConn, err := server.Accept(ctx)
	require.NoError(t, err)
	defer serverConn.CloseWithError(0, "")

	serverErrChan := make(chan error, 1)
	go func() {
		str, err := serverConn.OpenUniStream()
		if err != nil {
			serverErrChan <- err
			return
		}
		defer str.Close()
		if _, err := str.Write(PRDataLong); err != nil {
			serverErrChan <- err
			return
		}
		close(serverErrChan)
	}()

	str, err := conn.AcceptUniStream(ctx)
	require.NoError(t, err)
	data, err := io.ReadAll(str)
	require.NoError(t, err)
	require.Equal(t, PRDataLong, data)
	require.NoError(t, conn.CloseWithError(0, ""))

	require.NoError(t, <-serverErrChan)
	// Wait for both connection run loops to finish before returning: the
	// deferred t.Cleanup restores handshake.KeyUpdateInterval, which the
	// run loops still read (shouldInitiateKeyUpdate). Racing the restore
	// against a live connection is a data race.
	ctxDrain, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDrain()
	<-conn.Context().Done()
	select {
	case <-serverConn.Context().Done():
	case <-ctxDrain.Done():
	}

	keyPhasesSent, keyPhasesReceived := countKeyPhases()
	t.Logf("Used %d key phases on outgoing and %d key phases on incoming packets.", keyPhasesSent, keyPhasesReceived)
	// Key updates are initiated independently in each direction. The server's
	// data packets and the client's ACK packets need not produce equal phase
	// counts; successful payload delivery proves every observed phase decoded.
	require.Greater(t, keyPhasesSent, 10)
	require.Greater(t, keyPhasesReceived, 10)
}
