package blk

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	blkGetSize64 = 0x80081272
)

func deviceSizeBytes(f *os.File, st os.FileInfo) (uint64, error) {
	if st.Mode().IsRegular() {
		return uint64(st.Size()), nil
	}

	var size uint64
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), blkGetSize64, uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0, errno
	}
	return size, nil
}

func DeviceSizeBytes(f *os.File, st os.FileInfo) (uint64, error) {
	return deviceSizeBytes(f, st)
}

func IsBlockDevice(mode os.FileMode) bool {
	return mode&os.ModeDevice != 0
}
