package quic

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/mocks"
	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/wire"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// releaseAll must reach every queued entry, including frames sitting at unread
// gaps that Pop (which only yields the contiguous next frame) would not return.
func TestFrameSorterReleaseAll(t *testing.T) {
	s := newFrameSorter()
	cb1, tr1 := getFrameSorterTestCallback(t)
	cb2, tr2 := getFrameSorterTestCallback(t)
	// contiguous frame at readPos 0, then an out-of-order frame past a gap.
	require.NoError(t, s.Push([]byte("foo"), 0, cb1))
	require.NoError(t, s.Push([]byte("bar"), 10, cb2))
	require.True(t, s.HasMoreData())

	// Nothing popped yet: releaseAll must release both, gap included.
	s.releaseAll()
	require.False(t, s.HasMoreData())
	require.True(t, tr1.WasCalled(), "contiguous frame must be released")
	require.True(t, tr2.WasCalled(), "gap frame must be released")
	// Queue is empty after release.
	_, data, doneCb := s.Pop()
	require.Nil(t, data)
	require.Nil(t, doneCb)
}

// closeForShutdown must return the in-flight current frame and every queued
// frame (gap included) to the pool, exactly once each.
func TestReceiveStreamCloseForShutdownReleasesFrames(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockFC := mocks.NewMockStreamFlowController(mockCtrl)
	str := newReceiveStream(42, nil, mockFC)

	cbCur, trCur := getFrameSorterTestCallback(t)
	cbGap, trGap := getFrameSorterTestCallback(t)
	require.NoError(t, str.frameQueue.Push([]byte("current"), 0, cbCur))
	require.NoError(t, str.frameQueue.Push([]byte("gap"), 10, cbGap))

	// Pop the contiguous frame so it becomes the in-flight currentFrameDone.
	str.mutex.Lock()
	str.dequeueNextFrame()
	str.mutex.Unlock()
	require.False(t, trCur.WasCalled(), "current frame held until shutdown")
	require.False(t, trGap.WasCalled(), "gap frame held until shutdown")

	str.closeForShutdown(errors.New("shut down"))

	require.True(t, trCur.WasCalled(), "in-flight current frame must be released on shutdown")
	require.True(t, trGap.WasCalled(), "queued gap frame must be released on shutdown")
}

// A frame arriving after shutdown must not be queued (it would be stranded
// once releasePendingFrames has drained the queue).
func TestReceiveStreamRejectsFrameAfterShutdown(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockFC := mocks.NewMockStreamFlowController(mockCtrl)
	str := newReceiveStream(42, nil, mockFC)

	str.closeForShutdown(errors.New("shut down"))
	require.NoError(t, str.handleStreamFrame(
		&wire.StreamFrame{Data: []byte("late")}, time.Now()))
	require.False(t, str.frameQueue.HasMoreData(),
		"frame arriving after shutdown must not be queued")
}

// Reading to EOF releases the in-flight frame, but must also clear
// currentFrameDone: a later closeForShutdown (releasePendingFrames, e.g. on
// connection shutdown while the send half of a bidirectional stream is still
// open) would otherwise PutBack the same pointer a second time, and the
// global channel pool would hand one frame's Data buffer to two streams
// concurrently (data races, cross-stream corruption).
func TestReceiveStreamEOFThenShutdownSinglePut(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockFC := mocks.NewMockStreamFlowController(mockCtrl)
	mockFC.EXPECT().AddBytesRead(gomock.Any()).Return(false, false).AnyTimes()
	str := newReceiveStream(42, nil, mockFC)

	// Feed one pooled frame carrying the whole stream (FIN implied by
	// finalOffset) through the real queue path.
	f := wire.GetStreamFrame()
	copy(f.Data, "hello")
	f.Data = f.Data[:5]
	f.Offset = 0
	str.mutex.Lock()
	str.finalOffset = 5
	require.NoError(t, str.frameQueue.Push(f.Data, 0, f))
	// Drive the exact readImpl EOF branch (copy loop -> last-frame check).
	_, _, n, err := str.readImpl(make([]byte, 16))
	str.mutex.Unlock()
	require.Equal(t, 5, n)
	require.ErrorIs(t, err, io.EOF)

	str.closeForShutdown(errors.New("shut down"))
}

// closeForShutdown must unblock a waiting reader and surface the error; it
// must not deadlock from draining under the same mutex used by the read loop.
func TestReceiveStreamCloseForShutdownUnblocksReader(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockFC := mocks.NewMockStreamFlowController(mockCtrl)
	str := newReceiveStream(42, nil, mockFC)

	done := make(chan error, 1)
	go func() {
		_, err := str.Read(make([]byte, 10))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the reader block on readChan

	str.closeForShutdown(errors.New("shut down"))

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Read was not unblocked after closeForShutdown")
	}
}

func TestReceiveStreamCancelReadReleasesQueuedFrames(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockFC := mocks.NewMockStreamFlowController(mockCtrl)
	mockSender := NewMockStreamSender(mockCtrl)
	str := newReceiveStream(42, mockSender, mockFC)

	cbCur, trCur := getFrameSorterTestCallback(t)
	cbGap, trGap := getFrameSorterTestCallback(t)
	require.NoError(t, str.frameQueue.Push([]byte("current"), 0, cbCur))
	require.NoError(t, str.frameQueue.Push([]byte("gap"), 10, cbGap))
	str.mutex.Lock()
	str.dequeueNextFrame()
	str.mutex.Unlock()

	mockSender.EXPECT().onHasStreamControlFrame(str.StreamID(), gomock.Any())
	str.CancelRead(1234)

	require.True(t, trCur.WasCalled(), "in-flight current frame must be released on CancelRead")
	require.True(t, trGap.WasCalled(), "queued gap frame must be released on CancelRead")

	str.CancelRead(4321)
	n, err := str.Read([]byte{0})
	require.Zero(t, n)
	require.ErrorIs(t, err, &StreamError{StreamID: 42, ErrorCode: 1234, Remote: false})
}

func TestReceiveStreamResetReleasesQueuedFrames(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockFC := mocks.NewMockStreamFlowController(mockCtrl)
	mockSender := NewMockStreamSender(mockCtrl)
	str := newReceiveStream(42, mockSender, mockFC)

	cbCur, trCur := getFrameSorterTestCallback(t)
	cbGap, trGap := getFrameSorterTestCallback(t)
	require.NoError(t, str.frameQueue.Push([]byte("current"), 0, cbCur))
	require.NoError(t, str.frameQueue.Push([]byte("gap"), 10, cbGap))
	str.mutex.Lock()
	str.dequeueNextFrame()
	str.mutex.Unlock()

	mockFC.EXPECT().UpdateHighestReceived(protocol.ByteCount(42), true, gomock.Any())
	mockFC.EXPECT().Abandon()
	require.NoError(t, str.handleResetStreamFrame(
		&wire.ResetStreamFrame{StreamID: 42, ErrorCode: 7, FinalSize: 42},
		time.Now(),
	))

	require.True(t, trCur.WasCalled(), "in-flight current frame must be released on RESET_STREAM")
	require.True(t, trGap.WasCalled(), "queued gap frame must be released on RESET_STREAM")
}
