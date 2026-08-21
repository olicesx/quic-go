//go:build linux && (386 || arm || mips || mipsle)

package quic

import (
	"syscall"
	"unsafe"
)

// msghdrLenType matches syscall.Msghdr.Controllen on 32-bit platforms.
type msghdrLenType = uint32

// Compile-time assertion: msghdrLenType must be exactly as wide as
// syscall.Msghdr.Controllen, otherwise the assignment in ReadBatch would
// silently truncate buffer lengths.
var _ [unsafe.Sizeof(syscall.Msghdr{}.Controllen)]struct{} = [unsafe.Sizeof(msghdrLenType(0))]struct{}{}
