package quic

import (
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
)

// BenchmarkFrameSorterWithGaps measures the reordering path: frames arrive
// out of order, creating gaps in the B-tree that must be split/merged.
// This is what B-tree node pooling (254bec0d) targeted.
func BenchmarkFrameSorterWithGaps(b *testing.B) {
	fs := newFrameSorter()
	payload := make([]byte, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Deliver frames in reverse order within a 64-frame window:
		// every push splits an existing gap, exercising tree insert/delete.
		base := protocol.ByteCount(i%64) * 1200
		start := base + 1200*protocol.ByteCount(63-(i%64))
		err := fs.Push(payload, start, nil)
		if err != nil {
			b.Fatal(err)
		}
		if i%64 == 63 {
			// window complete: drain
			for j := 0; j < 64; j++ {
				_, _, _ = fs.Pop()
			}
		}
	}
}

// BenchmarkFrameSorterCleanup measures the path where the window is not
// drained - the sorter retains entries (like an idle stream after a burst).
func BenchmarkFrameSorterRetain(b *testing.B) {
	fs := newFrameSorter()
	payload := make([]byte, 1200)
	for i := 0; i < 100; i++ {
		start := protocol.ByteCount(i) * 1200
		fs.Push(payload, start, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := protocol.ByteCount(100+i) * 1200
		err := fs.Push(payload, start, nil)
		if err != nil {
			b.Fatal(err)
		}
		fs.Pop()
	}
}
