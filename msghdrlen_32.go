//go:build linux && (386 || arm || mips || mipsle)

package quic

// msghdrLenType is the field type of syscall.Msghdr.Controllen on this
// platform (32-bit: uint32).
type msghdrLenType = uint32
