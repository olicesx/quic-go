package self_test

import (
	"context"
	"io"
	"testing"
	"time"

	quic "github.com/olicesx/quic-go"
	"github.com/stretchr/testify/require"
)

func runDatagramBurst(t *testing.T, burst int) (sent, received int, clientErr, serverErr error, streamOK bool) {
	t.Helper()
	cfg := func() *quic.Config {
		return getQuicConfig(&quic.Config{
			EnableDatagrams: true,
			MaxIdleTimeout:  30 * time.Second,
			KeepAlivePeriod: 2 * time.Second,
		})
	}
	ln, err := quic.ListenAddr("localhost:0", getTLSConfig(), cfg())
	require.NoError(t, err)
	defer ln.Close()

	type acceptResult struct {
		conn quic.Connection
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept(context.Background())
		acceptCh <- acceptResult{conn: c, err: err}
	}()
	clientConn, err := quic.DialAddr(context.Background(), ln.Addr().String(), getTLSClientConfig(), cfg())
	require.NoError(t, err)
	defer clientConn.CloseWithError(0, "")
	acceptCtx, acceptCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer acceptCancel()
	var serverConn quic.Connection
	select {
	case res := <-acceptCh:
		require.NoError(t, res.err, "server accept failed")
		serverConn = res.conn
	case <-acceptCtx.Done():
		t.Fatal("server never accepted the connection")
	}
	defer serverConn.CloseWithError(0, "")

	payload := make([]byte, 900)
	for i := 0; i < burst; i++ {
		if err := clientConn.SendDatagram(payload); err != nil {
			break
		}
		sent++
	}
	time.Sleep(1500 * time.Millisecond)

	// Drain until no datagram arrives within a short window. The sender has
	// finished by now, so the count is the queue occupancy, not a race with the
	// send path; a mid-drain arrival would only make the observed count larger.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		_, err := serverConn.ReceiveDatagram(ctx)
		cancel()
		if err != nil {
			break
		}
		received++
	}
	clientErr = clientConn.Context().Err()
	serverErr = serverConn.Context().Err()

	stream, err := clientConn.OpenStreamSync(context.Background())
	if err != nil {
		t.Logf("step open failed: %v", err)
		return
	}

	if _, err = stream.Write([]byte("still-alive")); err != nil {
		t.Logf("step write failed: %v", err)
		return
	}
	_ = stream.Close()
	actx, acancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer acancel()
	accepted, err := serverConn.AcceptStream(actx)
	if err != nil {
		t.Logf("step accept failed: %v", err)
		return
	}
	// A short Read may legitimately return the data together with io.EOF, so
	// only the byte count and content decide the outcome.
	all, err := io.ReadAll(accepted)
	if err != nil {
		t.Logf("step read failed: %v", err)
		return
	}
	streamOK = string(all) == "still-alive"
	return
}

// Audit probe for the DATAGRAM receive queue bound (maxDatagramRcvQueueLen=512).
func TestDatagramReceiveQueueOverflow(t *testing.T) {
	t.Run("control-burst-100", func(t *testing.T) {
		sent, received, cerr, serr, ok := runDatagramBurst(t, 100)
		t.Logf("burst=100 sent=%d received=%d clientErr=%v serverErr=%v streamOK=%v", sent, received, cerr, serr, ok)
		require.Equal(t, 100, sent, "SendDatagram must accept a sub-limit burst")
		require.Equal(t, 100, received, "a sub-limit burst must not be dropped")
		require.NoError(t, cerr, "client connection must stay alive")
		require.NoError(t, serr, "server connection must stay alive")
		require.True(t, ok, "connection must survive a sub-limit burst")
	})
	t.Run("overflow-burst-1200", func(t *testing.T) {
		sent, received, cerr, serr, ok := runDatagramBurst(t, 1200)
		t.Logf("burst=1200 sent=%d received=%d clientErr=%v serverErr=%v streamOK=%v", sent, received, cerr, serr, ok)
		require.Equal(t, 1200, sent, "SendDatagram must accept the whole burst")
		// The receive queue holds at most maxDatagramRcvQueueLen (512); the
		// overflow is dropped in the kernel-independent queue, so fewer than
		// sent must arrive. Assert the bound (not an exact count) so a slower
		// machine that loses a few more datagrams cannot flake, while a raised
		// bound still fails here.
		require.LessOrEqual(t, received, 512, "receive queue bound must not exceed maxDatagramRcvQueueLen")
		require.Greater(t, received, 0, "the receiver must still get a prefix of the burst")
		require.Less(t, received, sent, "the overflow must be dropped, not delivered")
		require.NoError(t, cerr, "client connection must stay alive")
		require.NoError(t, serr, "server connection must stay alive")
		require.True(t, ok, "connection must survive an overflow burst")
	})
}
