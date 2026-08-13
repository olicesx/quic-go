package handshake

import (
	"crypto"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/exp/rand"
)

// hkdfExpandLabelReference is a straightforward implementation of
// HKDF-Expand-Label from RFC 8446, Section 7.1, built only on public APIs:
//
//	HKDF-Expand-Label(Secret, Label, Context, Length) =
//	    HKDF-Expand(Secret, HkdfLabel, Length)
//
// It serves as the correctness oracle for the optimized hkdfExpandLabel. It
// replaces a //go:linkname hook into crypto/tls internals
// (crypto/tls.(*cipherSuiteTLS13).expandLabel) that no longer exists in
// Go 1.25+.
func hkdfExpandLabelReference(hash crypto.Hash, secret, context []byte, label string, length int) []byte {
	labelWithPrefix := "tls13 " + label
	hkdfLabel := make([]byte, 2+1+len(labelWithPrefix)+1+len(context))
	binary.BigEndian.PutUint16(hkdfLabel[:2], uint16(length))
	hkdfLabel[2] = uint8(len(labelWithPrefix))
	offset := 3
	copy(hkdfLabel[offset:], labelWithPrefix)
	offset += len(labelWithPrefix)
	hkdfLabel[offset] = uint8(len(context))
	offset++
	copy(hkdfLabel[offset:], context)

	out := make([]byte, length)
	r := hkdf.Expand(hash.New, secret, hkdfLabel)
	if _, err := io.ReadFull(r, out); err != nil {
		panic(err)
	}
	return out
}

func TestHKDF(t *testing.T) {
	testCases := []struct {
		name string
		hash crypto.Hash
	}{
		{"TLS_AES_128_GCM_SHA256", crypto.SHA256},
		{"TLS_AES_256_GCM_SHA384", crypto.SHA384},
		{"TLS_CHACHA20_POLY1305_SHA256", crypto.SHA256},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			secret := []byte("secret")
			context := []byte("context")
			label := "label"
			length := 42
			expected := hkdfExpandLabelReference(tc.hash, secret, context, label, length)
			expanded := hkdfExpandLabel(tc.hash, secret, context, label, length)
			require.Equal(t, expected, expanded)
		})
	}
}

func BenchmarkHKDFExpandLabelReference(b *testing.B) {
	benchmarkHKDFExpandLabel(b, crypto.SHA256, true)
}

func BenchmarkHKDFExpandLabelOptimized(b *testing.B) {
	benchmarkHKDFExpandLabel(b, crypto.SHA256, false)
}

func benchmarkHKDFExpandLabel(b *testing.B, hash crypto.Hash, useReference bool) {
	b.ReportAllocs()
	secret := make([]byte, 32)
	rand.Read(secret)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if useReference {
			hkdfExpandLabelReference(hash, secret, []byte("context"), "label", 42)
		} else {
			hkdfExpandLabel(hash, secret, []byte("context"), "label", 42)
		}
	}
}
