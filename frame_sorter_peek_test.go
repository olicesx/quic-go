package quic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrameSorterPeekContiguous(t *testing.T) {
	s := newFrameSorter()
	require.Zero(t, s.PeekContiguous(), "empty sorter peeks zero")

	require.NoError(t, s.Push([]byte("foo"), 0, nil))
	require.Equal(t, 3, s.PeekContiguous(), "contiguous frame is peeked in full")

	// A frame sitting behind a gap is not contiguous and must not be
	// peeked: the next Pop would block waiting for the gap to fill.
	require.NoError(t, s.Push([]byte("bar"), 7, nil))
	require.Equal(t, 3, s.PeekContiguous(), "frame behind a gap is not peeked")

	_, data, _ := s.Pop()
	require.Equal(t, []byte("foo"), data)
	require.Zero(t, s.PeekContiguous(), "gap at read position peeks zero")

	require.NoError(t, s.Push([]byte("baz"), 3, nil))
	require.Equal(t, 3, s.PeekContiguous())
	// Peeking is observational: repeated peeks and a subsequent Pop agree.
	require.Equal(t, 3, s.PeekContiguous())
	_, data, _ = s.Pop()
	require.Equal(t, []byte("baz"), data)
}
