//go:build linux && (amd64 || arm64 || riscv64 || loong64 || ppc64le || s390x)

package quic

// msghdrLenType is the field type of syscall.Msghdr.Controllen on this
// platform (64-bit: uint64).
type msghdrLenType = uint64
