package qlog

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

type limitedWriter struct {
	io.WriteCloser
	N       int
	written int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.N {
		return 0, errors.New("writer full")
	}
	n, err := w.WriteCloser.Write(p)
	w.written += n
	return n, err
}

func TestWritingStopping(t *testing.T) {
	buf := &bytes.Buffer{}
	t.Run("stops writing when encountering an error", func(t *testing.T) {
		tracer := NewConnectionTracer(
			&limitedWriter{WriteCloser: nopWriteCloser(buf), N: 250},
			protocol.PerspectiveServer,
			protocol.ParseConnectionID([]byte{0xde, 0xad, 0xbe, 0xef}),
		)

		for i := uint32(0); i < 1000; i++ {
			tracer.UpdatedPTOCount(i)
		}

		// Capture log output
		var logBuf bytes.Buffer
		log.SetOutput(&logBuf)
		defer log.SetOutput(os.Stdout)

		tracer.Close()

		require.Contains(t, logBuf.String(), "writer full")
	})
}

// blockingWriter blocks until release is closed.
type blockingWriter struct {
	release chan struct{}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func (w *blockingWriter) Close() error { return nil }

// Recording an event must never block the caller (the connection's hot path):
// if the writer can't keep up, events are dropped and counted.
func TestWriterDropsEventsWhenTheWriterIsBlocked(t *testing.T) {
	blocked := &blockingWriter{release: make(chan struct{})}
	tr := &trace{
		VantagePoint: vantagePoint{Type: "transport"},
		CommonFields: commonFields{ReferenceTime: time.Now()},
	}
	w := newWriter(blocked, tr)
	go w.Run()
	// The Run goroutine is blocked writing the header, so the event channel
	// fills up and every further event is dropped.
	for i := 0; i < eventChanSize+1; i++ {
		w.RecordEvent(time.Now(), &eventGeneric{name: "test", msg: "test"})
	}
	require.Equal(t, uint64(1), w.DroppedEvents())

	// Capture the log output of Close.
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stdout)
	close(blocked.release)
	w.Close()
	require.Contains(t, logBuf.String(), "dropped 1 events")
}

// lockedBuffer is a bytes.Buffer that tolerates being read while the writer
// goroutine appends to it, so a test can observe the asynchronous writer
// without racing on the buffer itself.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) contains(s string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Contains(b.buf.Bytes(), []byte(s))
}

// A RecordEvent racing Close must neither panic — the previous design closed
// the events channel, turning a racing sender into a send on a closed
// channel — nor lose the events that were already recorded. Late senders are
// dropped once Run has drained, and Close is idempotent.
func TestWriterRecordEventRacingCloseDoesNotPanic(t *testing.T) {
	buf := &lockedBuffer{}
	tr := &trace{
		VantagePoint: vantagePoint{Type: "transport"},
		CommonFields: commonFields{ReferenceTime: time.Now()},
	}
	w := newWriter(nopWriteCloser(buf), tr)
	go w.Run()

	// Establish the property under test before racing Close: an event handed to
	// the writer before Close is written out. Run encodes asynchronously, so
	// wait for it instead of assuming a scheduling order.
	w.RecordEvent(time.Now(), &eventGeneric{name: "before-close", msg: "before-close"})
	require.Eventually(t, func() bool { return buf.contains("before-close") },
		time.Second, time.Millisecond, "event recorded before Close was not written")

	// Race a burst of senders against Close. The interleaving is deliberately
	// not synchronized: whatever the timing, this must not panic and Close must
	// be idempotent.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			w.RecordEvent(time.Now(), &eventGeneric{name: "racing", msg: "racing"})
		}
	}()
	// Capture the Close log line: the racing burst is expected to overrun the
	// event channel, and that is reported through the standard logger.
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stdout)
	w.Close()
	<-done
	require.NotPanics(t, func() { w.Close() })

	// Run has exited, so a late event is dropped instead of being encoded, and
	// the trace still holds what was recorded before Close.
	before := buf.Len()
	w.RecordEvent(time.Now(), &eventGeneric{name: "after-close", msg: "after-close"})
	require.Equal(t, before, buf.Len())
	require.True(t, buf.contains("before-close"))
	require.NotContains(t, buf.String(), "after-close")
}
