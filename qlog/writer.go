package qlog

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/francoispqt/gojay"
)

const eventChanSize = 50

const recordSeparator = 0x1e

func writeRecordSeparator(w io.Writer) error {
	_, err := w.Write([]byte{recordSeparator})
	return err
}

type writer struct {
	w io.WriteCloser

	referenceTime time.Time
	tr            *trace

	events     chan event
	encodeErr  error
	runStopped chan struct{}

	// droppedEvents counts qlog events that were dropped because the writer
	// (e.g. a slow disk) couldn't keep up. Event recording must never block the
	// connection, so the drop is counted and reported - not silent.
	// It's a pointer, because connectionTracer copies the writer struct; all
	// copies share one counter.
	droppedEvents *atomic.Uint64
}

func newWriter(w io.WriteCloser, tr *trace) *writer {
	return &writer{
		w:             w,
		tr:            tr,
		referenceTime: tr.CommonFields.ReferenceTime,
		runStopped:    make(chan struct{}),
		events:        make(chan event, eventChanSize),
		droppedEvents: &atomic.Uint64{},
	}
}

func (w *writer) RecordEvent(eventTime time.Time, details eventDetails) {
	ev := event{
		RelativeTime: eventTime.Sub(w.referenceTime),
		eventDetails: details,
	}
	// Never block the caller (this runs on the connection's hot path):
	// if the event channel is full, the writer is too slow, and the event is
	// dropped and counted.
	select {
	case w.events <- ev:
	default:
		w.droppedEvents.Add(1)
	}
}

// DroppedEvents returns the number of events dropped so far.
func (w *writer) DroppedEvents() uint64 { return w.droppedEvents.Load() }

func (w *writer) Run() {
	defer close(w.runStopped)
	buf := &bytes.Buffer{}
	enc := gojay.NewEncoder(buf)
	if err := writeRecordSeparator(buf); err != nil {
		panic(fmt.Sprintf("qlog encoding into a bytes.Buffer failed: %s", err))
	}
	if err := enc.Encode(&topLevel{trace: *w.tr}); err != nil {
		panic(fmt.Sprintf("qlog encoding into a bytes.Buffer failed: %s", err))
	}
	if err := buf.WriteByte('\n'); err != nil {
		panic(fmt.Sprintf("qlog encoding into a bytes.Buffer failed: %s", err))
	}
	if _, err := w.w.Write(buf.Bytes()); err != nil {
		w.encodeErr = err
	}
	enc = gojay.NewEncoder(w.w)
	for ev := range w.events {
		if w.encodeErr != nil { // if encoding failed, just continue draining the event channel
			continue
		}
		if err := writeRecordSeparator(w.w); err != nil {
			w.encodeErr = err
			continue
		}
		if err := enc.Encode(ev); err != nil {
			w.encodeErr = err
			continue
		}
		if _, err := w.w.Write([]byte{'\n'}); err != nil {
			w.encodeErr = err
		}
	}
}

func (w *writer) Close() {
	if n := w.droppedEvents.Load(); n > 0 {
		log.Printf("qlog: dropped %d events: the writer couldn't keep up\n", n)
	}
	if err := w.close(); err != nil {
		log.Printf("exporting qlog failed: %s\n", err)
	}
}

func (w *writer) close() error {
	close(w.events)
	<-w.runStopped
	if w.encodeErr != nil {
		return w.encodeErr
	}
	return w.w.Close()
}
