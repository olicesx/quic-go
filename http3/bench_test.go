package http3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/olicesx/qpack"
	"github.com/olicesx/quic-go"
)

// benchmarkStream is a minimal quic.Stream implementation. The tests use
// gomock streams, whose per-call reflection overhead would dominate these
// benchmarks.
type benchmarkStream struct {
	id  quic.StreamID
	buf bytes.Buffer
}

var _ quic.Stream = &benchmarkStream{}

func (s *benchmarkStream) StreamID() quic.StreamID          { return s.id }
func (s *benchmarkStream) Context() context.Context         { return context.Background() }
func (s *benchmarkStream) Read(b []byte) (int, error)       { return s.buf.Read(b) }
func (s *benchmarkStream) Write(b []byte) (int, error)      { return s.buf.Write(b) }
func (s *benchmarkStream) Close() error                     { return nil }
func (s *benchmarkStream) CancelRead(quic.StreamErrorCode)  {}
func (s *benchmarkStream) CancelWrite(quic.StreamErrorCode) {}
func (s *benchmarkStream) SetReadDeadline(time.Time) error  { return nil }
func (s *benchmarkStream) SetWriteDeadline(time.Time) error { return nil }
func (s *benchmarkStream) SetDeadline(time.Time) error      { return nil }

func benchmarkRequest() *http.Request {
	req, err := http.NewRequest(http.MethodGet, "https://quic-go.net/index.html", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add("Accept", "text/html,application/xhtml+xml")
	req.Header.Add("Accept-Language", "en-US,en;q=0.9")
	req.Header.Add("User-Agent", "benchmark")
	return req
}

// encodes a request header (QPACK encoding + HEADERS frame header).
// The per-call bytes.Buffer that WriteRequestHeader allocates is included.
func BenchmarkWriteRequestHeader(b *testing.B) {
	req := benchmarkRequest()
	str := &benchmarkStream{id: 4}
	rw := newRequestWriter()
	str.buf.Grow(4096)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		str.buf.Reset()
		if err := rw.WriteRequestHeader(str, req, false); err != nil {
			b.Fatal(err)
		}
	}
}

// encodeFieldSection QPACK-encodes the given header fields.
func encodeFieldSection(b *testing.B, fields []qpack.HeaderField) []byte {
	b.Helper()
	buf := &bytes.Buffer{}
	enc := qpack.NewEncoder(buf)
	for _, f := range fields {
		if err := enc.WriteField(f); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// decodes a response header block: QPACK decoding + conversion into an
// http.Response (the hot path when reading a response).
func BenchmarkParseResponseHeaders(b *testing.B) {
	encoded := encodeFieldSection(b, []qpack.HeaderField{
		{Name: ":status", Value: "200"},
		{Name: "content-type", Value: "text/html; charset=utf-8"},
		{Name: "content-encoding", Value: "gzip"},
		{Name: "date", Value: "Mon, 02 Jan 2006 15:04:05 GMT"},
		{Name: "cache-control", Value: "max-age=3600"},
	})

	decoder := qpack.NewDecoder()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rsp := &http.Response{}
		if err := updateResponseFromHeadersIncremental(rsp, decoder.Decode(encoded), 10*1<<20); err != nil {
			b.Fatal(err)
		}
	}
}

// decodes a trailer section. The HTTP/3 server decodes client trailers since
// fbf90cb0, so this is a remotely triggerable path.
func BenchmarkParseTrailers(b *testing.B) {
	encoded := encodeFieldSection(b, []qpack.HeaderField{
		{Name: "expires", Value: "Mon, 02 Jan 2006 15:04:05 GMT"},
		{Name: "etag", Value: "\"abc123\""},
	})

	decoder := qpack.NewDecoder()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := parseTrailersIncremental(decoder.Decode(encoded), 10*1<<20); err != nil {
			b.Fatal(err)
		}
	}
}

// reads the payload of a DATA frame, including the frame header parsing.
func BenchmarkStreamRead(b *testing.B) {
	payload := bytes.Repeat([]byte("a"), 1200)
	frame := (&dataFrame{Length: uint64(len(payload))}).Append(nil)
	frame = append(frame, payload...)

	str := &benchmarkStream{id: 0}
	s := newStream(str, nil, nil, func(io.Reader, uint64) error { return nil })
	buf := make([]byte, len(payload))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str.buf.Write(frame)
		if _, err := io.ReadFull(s, buf); err != nil {
			b.Fatal(err)
		}
	}
}

// parses the HTTP/3 frame header of a DATA frame.
func BenchmarkFrameParserParseNext(b *testing.B) {
	frame := (&dataFrame{Length: 1200}).Append(nil)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fp := &frameParser{r: bytes.NewReader(frame)}
		f, err := fp.ParseNext()
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := f.(*dataFrame); !ok {
			b.Fatalf("unexpected frame type %T", f)
		}
	}
}
