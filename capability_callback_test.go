package quic

import (
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/wire"

	"github.com/stretchr/testify/require"
)

// newCapabilityCallbackTestMap returns a streams map whose capability callback
// is the given function.
func newCapabilityCallbackTestMap(callback func(n int64)) *outgoingStreamsMap[*mockGenericStream] {
	return newOutgoingStreamsMap[*mockGenericStream](
		protocol.StreamTypeBidi,
		func(num protocol.StreamNum) *mockGenericStream { return &mockGenericStream{num: num} },
		func(wire.Frame) {},
		callback,
	)
}

// The capability callback is documented to run while the streams map lock is
// held. TryLock is used to observe that fact: Go mutexes are not reentrant, so
// a failing TryLock proves that the lock is held (by this goroutine).
func TestCapabilityCallbackRunsWithTheStreamsMapLockHeld(t *testing.T) {
	var m *outgoingStreamsMap[*mockGenericStream]
	lockStates := []bool{}
	callback := func(int64) {
		if m.mutex.TryLock() {
			m.mutex.Unlock()
			lockStates = append(lockStates, false)
			return
		}
		lockStates = append(lockStates, true)
	}
	m = newCapabilityCallbackTestMap(callback)

	// processing a MAX_STREAMS frame (run loop)
	m.SetMaxStream(3)
	require.Len(t, lockStates, 1)
	require.True(t, lockStates[0], "callback must run with the streams map lock held")

	// opening a stream (application goroutine)
	_, err := m.OpenStream()
	require.NoError(t, err)
	require.Len(t, lockStates, 2)
	require.True(t, lockStates[1], "callback must run with the streams map lock held")
}

// The counterpart of the contract test: calling back into the connection from
// the callback deadlocks, because the callback holds the streams map lock.
// The documented misuse must not silently work, so this test pins it down.
func TestCapabilityCallbackMustNotCallBackIntoTheConnection(t *testing.T) {
	var m *outgoingStreamsMap[*mockGenericStream]
	returned := make(chan struct{})
	inCallback := false
	callback := func(int64) {
		if inCallback { // only the first invocation is interesting
			return
		}
		inCallback = true
		go func() {
			defer close(returned)
			// OpenStream needs the lock that the callback is holding.
			_, _ = m.OpenStream()
		}()
		select {
		case <-returned:
			t.Error("OpenStream returned from within the capability callback: the streams map lock is not held")
		case <-time.After(scaleDuration(20 * time.Millisecond)):
			// expected: the connection API is blocked
		}
	}
	m = newCapabilityCallbackTestMap(callback)

	m.SetMaxStream(1)
}
