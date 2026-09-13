package self_test

import (
	"context"
	"io"
	"testing"
	"time"

	quic "github.com/olicesx/quic-go"
	"github.com/stretchr/testify/require"
)

// Isolate what makes the stream read fail: datagrams present, drain loop, or
// nothing at all.
func TestStreamWriteCloseThenRead(t *testing.T) {
	cases := []struct {
		name       string
		datagrams  int
		drainFirst bool
	}{
		{"no-datagrams", 0, false},
		{"datagrams-no-drain", 100, false},
		{"datagrams-with-drain", 100, true},
		{"datagrams-with-drain-1200", 1200, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := func() *quic.Config {
				return getQuicConfig(&quic.Config{MaxIdleTimeout: 30 * time.Second, EnableDatagrams: true})
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
			var serverConn quic.Connection
			select {
			case res := <-acceptCh:
				require.NoError(t, res.err, "server accept failed")
				serverConn = res.conn
			case <-time.After(10 * time.Second):
				t.Fatal("server never accepted the connection")
			}
			defer serverConn.CloseWithError(0, "")

			if tc.datagrams > 0 {
				payload := make([]byte, 900)
				for i := 0; i < tc.datagrams; i++ {
					require.NoError(t, clientConn.SendDatagram(payload))
				}
				time.Sleep(500 * time.Millisecond)
			}
			if tc.drainFirst {
				deadline := time.Now().Add(1 * time.Second)
				got := 0
				for time.Now().Before(deadline) {
					ctx, cancel := context.WithDeadline(context.Background(), deadline)
					_, err := serverConn.ReceiveDatagram(ctx)
					cancel()
					if err != nil {
						break
					}
					got++
				}
				t.Logf("drained %d datagrams", got)
			}

			writeErrCh := make(chan error, 1)
			go func() {
				st, err := clientConn.OpenStreamSync(context.Background())
				if err == nil {
					_, err = st.Write([]byte("still-alive"))
				}
				if err == nil {
					err = st.Close()
				}
				writeErrCh <- err
			}()
			actx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			accepted, err := serverConn.AcceptStream(actx)
			require.NoError(t, err)
			all, err := io.ReadAll(accepted)
			t.Logf("io.ReadAll -> %q err=%v", string(all), err)
			require.NoError(t, err)
			require.Equal(t, "still-alive", string(all))
			// The client side must report no error either: a Write or Close
			// failure otherwise only shows up as the read above timing out.
			select {
			case werr := <-writeErrCh:
				require.NoError(t, werr, "client OpenStream/Write/Close failed")
			case <-time.After(5 * time.Second):
				t.Fatal("client stream goroutine never finished")
			}
		})
	}
}
