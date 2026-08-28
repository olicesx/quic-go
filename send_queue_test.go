package quic

import (
	"errors"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func getPacketWithContents(b []byte) *packetBuffer {
	buf := getPacketBuffer()
	buf.Data = buf.Data[:len(b)]
	copy(buf.Data, b)
	return buf
}

func TestSendQueueSendOnePacket(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	c := NewMockSendConn(mockCtrl)
	q := newSendQueue(c)

	written := make(chan struct{})
	c.EXPECT().Write([]byte("foobar"), uint16(10), protocol.ECT1).Do(
		func([]byte, uint16, protocol.ECN) error { close(written); return nil },
	)

	done := make(chan struct{})
	go func() {
		q.Run()
		close(done)
	}()

	q.Send(getPacketWithContents([]byte("foobar")), 10, protocol.ECT1)

	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	q.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestSendQueueBlocking(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	c := NewMockSendConn(mockCtrl)
	q := newSendQueue(c)

	blockWrite := make(chan struct{})
	written := make(chan struct{}, 1)
	c.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func([]byte, uint16, protocol.ECN) error {
			select {
			case written <- struct{}{}:
			default:
			}
			<-blockWrite
			return nil
		},
	).AnyTimes()

	done := make(chan struct{})
	go func() {
		q.Run()
		close(done)
	}()

	// +1, since one packet will be queued in the Write call
	for i := 0; i < sendQueueCapacity+1; i++ {
		require.False(t, q.WouldBlock())
		q.Send(getPacketWithContents([]byte("foobar")), 10, protocol.ECT1)
		// make sure that the first packet is actually enqueued in the Write call
		if i == 0 {
			select {
			case <-written:
			case <-time.After(time.Second):
				t.Fatal("timeout")
			}
		}
	}
	require.True(t, q.WouldBlock())
	select {
	case <-q.Available():
		t.Fatal("should not be available")
	default:
	}
	require.Panics(t, func() { q.Send(getPacketWithContents([]byte("foobar")), 10, protocol.ECT1) })

	// allow one packet to be sent
	blockWrite <- struct{}{}
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	select {
	case <-q.Available():
		require.False(t, q.WouldBlock())
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// when calling Close, all packets are first sent out
	closed := make(chan struct{})
	go func() {
		q.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close should have blocked")
	case <-time.After(scaleDuration(10 * time.Millisecond)):
	}

	for i := 0; i < sendQueueCapacity; i++ {
		blockWrite <- struct{}{}
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestSendQueueWriteError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	c := NewMockSendConn(mockCtrl)
	q := newSendQueue(c)

	c.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	q.Send(getPacketWithContents([]byte("foobar")), 6, protocol.ECNNon)

	errChan := make(chan error, 1)
	go func() { errChan <- q.Run() }()

	select {
	case err := <-errChan:
		require.EqualError(t, err, "test error")
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Further calls must release their buffers and surface the fatal sender
	// error so the connection doesn't retain phantom sent-packet state.
	for i := 0; i < 2*sendQueueCapacity; i++ {
		buf := getPacketWithContents([]byte("raboof"))
		require.EqualError(t, q.Send(buf, 6, protocol.ECNNon), "test error")
		require.Zero(t, buf.refCount)
	}
}

func TestSendQueueFatalWriteDrainsAndReleasesBuffers(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	c := NewMockSendConn(mockCtrl)
	q := newSendQueue(c).(*sendQueue)

	buffers := make([]*packetBuffer, 0, sendQueueCapacity)
	for i := 0; i < sendQueueCapacity; i++ {
		buf := getPacketWithContents([]byte{byte(i)})
		buffers = append(buffers, buf)
		q.Send(buf, uint16(1200+i), protocol.ECT0)
	}
	c.EXPECT().Write([]byte{0}, uint16(1200), protocol.ECT0).Return(errors.New("fatal write"))
	require.EqualError(t, q.Run(), "fatal write")
	require.EqualError(t, q.LastRunError(), "fatal write")
	select {
	case <-q.RunStopped():
	default:
		t.Fatal("RunStopped must close when Run exits")
	}

	for _, buf := range buffers {
		require.Zero(t, buf.refCount, "buffer was not released after fatal write")
	}
}

func TestSendQueueSerializesSendAgainstStopDrain(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	c := NewMockSendConn(mockCtrl)
	q := newSendQueue(c).(*sendQueue)
	fatal := getPacketWithContents([]byte("fatal"))
	q.Send(fatal, 1200, protocol.ECNNon)

	writeStarted := make(chan struct{})
	c.EXPECT().Write([]byte("fatal"), uint16(1200), protocol.ECNNon).DoAndReturn(
		func([]byte, uint16, protocol.ECN) error {
			close(writeStarted)
			return errors.New("fatal write")
		},
	)
	// Hold a reader so Run reaches its stopping writer lock deterministically.
	q.stateMu.RLock()
	runDone := make(chan error, 1)
	go func() { runDone <- q.Run() }()
	<-writeStarted

	late := getPacketWithContents([]byte("late"))
	sendErr := make(chan error, 1)
	go func() { sendErr <- q.Send(late, 1200, protocol.ECNNon) }()
	q.stateMu.RUnlock()

	require.EqualError(t, <-runDone, "fatal write")
	select {
	case err := <-sendErr:
		require.EqualError(t, err, "fatal write")
	case <-time.After(time.Second):
		t.Fatal("Send did not observe the stopped queue")
	}
	require.Zero(t, fatal.refCount)
	require.Zero(t, late.refCount)
}

func TestSendQueueCloseIsIdempotent(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	q := newSendQueue(NewMockSendConn(mockCtrl))
	go func() { require.NoError(t, q.Run()) }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Close()
		q.Close()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idempotent Close blocked")
	}
}
