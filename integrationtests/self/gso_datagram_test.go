package self_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"

	quic "github.com/olicesx/quic-go"
	"github.com/stretchr/testify/require"

	"testing"
	"time"
)

// TestDatagramGSOBurst verifies that a high-volume stream of same-size
// datagrams survives the GSO merge path (sendPacketsWithGSO now merges
// same-size non-full packets): every datagram must arrive intact, with no
// kernel-side mis-segmentation.
//
// The sender is paced so the receive queue (cap 128) never overflows -
// datagrams are best-effort by design (RFC 9221) and a burst beyond the
// queue would be dropped by the receiver, not by GSO.
func TestDatagramGSOBurst(t *testing.T) {
	server, err := quic.Listen(
		newUPDConnLocalhost(t),
		getTLSConfig(),
		getQuicConfig(&quic.Config{EnableDatagrams: true}),
	)
	require.NoError(t, err)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientConn, err := quic.Dial(
		ctx,
		newUPDConnLocalhost(t),
		server.Addr(),
		getTLSClientConfig(),
		getQuicConfig(&quic.Config{EnableDatagrams: true}),
	)
	require.NoError(t, err)
	defer clientConn.CloseWithError(0, "")

	serverConn, err := server.Accept(ctx)
	require.NoError(t, err)
	defer serverConn.CloseWithError(0, "")

	const payloadLen = 1000 // same-size packets: the GSO merge condition
	const count = 600       // several GSO batches (20KB/1000 ≈ 20 per batch)
	payload := make([]byte, payloadLen)
	rand.Read(payload)

	recvErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			data, err := serverConn.ReceiveDatagram(ctx)
			if err != nil {
				recvErr <- fmt.Errorf("ReceiveDatagram: %w", err)
				return
			}
			if !bytes.Equal(data, payload) {
				recvErr <- fmt.Errorf("datagram %d corrupted: len=%d", i, len(data))
				return
			}
		}
		recvErr <- nil
	}()

	for i := 0; i < count; i++ {
		if err := clientConn.SendDatagram(payload); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		if i%50 == 49 {
			// let the receiver drain the queue; datagrams beyond the
			// receive-queue cap would be dropped by design
			time.Sleep(2 * time.Millisecond)
		}
	}
	clientConn.SendDatagram(payload) // nudge: ensure the queue flushes

	wg.Wait()
	require.NoError(t, <-recvErr)

}
