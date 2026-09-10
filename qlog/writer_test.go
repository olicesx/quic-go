package qlog

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
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
