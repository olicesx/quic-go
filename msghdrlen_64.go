//go:build linux && (amd64 || arm64 || loong64 || ppc64 || ppc64le || riscv64 || s390x || mips64 || mips64le)

package quic

import (
	"syscall"
	"unsafe"
)

// msghdrLenType matches syscall.Msghdr.Controllen on 64-bit platforms.
type msghdrLenType = uint64

// Compile-time assertion: msghdrLenType must be exactly as wide as
// syscall.Msghdr.Controllen, otherwise the assignment in ReadBatch would
// silently truncate buffer lengths.
var _ [unsafe.Sizeof(syscall.Msghdr{}.Controllen)]struct{} = [unsafe.Sizeof(msghdrLenType(0))]struct{}{}
