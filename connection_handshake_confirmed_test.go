package quic

import (
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// Audit check for CVE-2025-59530 (GHSA-47m2-4cr7-mhcw): a malicious server can
// send HANDSHAKE_DONE before the handshake really completed. The upstream fix
// drops the Initial keys as part of handling that frame, so a later Initial
// packet is classified as "keys dropped" instead of reaching the
// queue-undecryptable path, which asserts after handshake completion.
//
// The real frame handler is driven (not handleHandshakeConfirmed), so this
// covers the path a received HANDSHAKE_DONE frame takes. Only the key-drop
// consequence is asserted: handleUnpackError returns false for ErrKeysDropped
// regardless of droppedInitialKeys, so re-asserting "not queued" would pass even
// with the drop removed and would not guard anything.
func TestHandshakeConfirmedDropsInitialKeys(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	tc := newClientTestConnection(t, mockCtrl, nil, false)
	require.False(t, tc.conn.droppedInitialKeys, "precondition: Initial keys are still held")
	require.False(t, tc.conn.handshakeComplete, "precondition: handshake not complete yet")
	require.False(t, tc.conn.handshakeConfirmed, "precondition: handshake not confirmed yet")

	require.NoError(t, tc.conn.handleHandshakeDoneFrame(tc.conn.creationTime))
	require.True(t, tc.conn.handshakeConfirmed, "the HANDSHAKE_DONE frame must confirm the handshake")
	require.True(t, tc.conn.droppedInitialKeys,
		"handling HANDSHAKE_DONE must drop Initial keys (CVE-2025-59530 backport)")

	// A server must reject HANDSHAKE_DONE as a protocol violation, so the drop
	// stays tied to a legitimate client-side confirmation.
	tc.conn.handshakeConfirmed = false
	tc.conn.perspective = protocol.PerspectiveServer
	require.Error(t, tc.conn.handleHandshakeDoneFrame(tc.conn.creationTime))
}
