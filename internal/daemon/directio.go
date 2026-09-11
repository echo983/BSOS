package daemon

import (
	"fmt"
	"io"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/echo983/BSOS/internal/blk"
)

// directChunkBytes is the fixed size of the page-aligned scratch buffer
// used to stream bytes into an O_DIRECT fd. It bounds this path's peak
// memory to O(chunk size) regardless of object size (docs/DESIGN.md
// §3.3) — unlike NBSS's writeDataDirect, which took a fully-materialized
// []byte. Must be a multiple of blk.SlotSize (asserted at use).
const directChunkBytes = 1 << 20

// openDirectFile is a var, not a plain func, so tests can substitute a
// fake without depending on this host's filesystem actually supporting
// O_DIRECT.
var openDirectFile = func(path string) (deviceFile, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open O_DIRECT %s: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open O_DIRECT %s: nil file", path)
	}
	return f, nil
}

// writeStreamDirect streams realSize real bytes from r, followed by zero
// padding up to total, into f (an O_DIRECT-opened deviceFile) starting at
// addr. addr and total must already be blk.SlotSize-aligned (guaranteed
// by blk.AddrForFID and WriteFromContext's slotsNeeded*blk.SlotSize) —
// that invariant is asserted here, not re-derived. realSize itself need
// not be aligned; every individual chunk write is aligned in offset,
// length, and buffer address regardless of where the real/pad boundary
// falls inside it.
//
// Unlike NBSS's writeDataDirect (which takes a fully-materialized []byte,
// reintroducing the whole-object server-side buffering this system's
// streaming Put was built to eliminate), this reads incrementally from r
// into a single reused aligned buffer — peak memory is one
// directChunkBytes-sized buffer, independent of realSize/total.
func writeStreamDirect(f io.WriterAt, addr uint64, realSize uint64, total uint64, r io.Reader) error {
	if total%blk.SlotSize != 0 {
		return fmt.Errorf("direct write size %d not aligned", total)
	}
	if directChunkBytes%blk.SlotSize != 0 {
		return fmt.Errorf("direct chunk %d not aligned", directChunkBytes)
	}
	buf := alignedBuffer(directChunkBytes, blk.SlotSize)

	var written uint64
	for written < total {
		chunkLen := uint64(directChunkBytes)
		if remaining := total - written; remaining < chunkLen {
			chunkLen = remaining
		}
		var n uint64
		if written < realSize {
			want := realSize - written
			if want > chunkLen {
				want = chunkLen
			}
			got, err := io.ReadFull(r, buf[:want])
			n = uint64(got)
			if n != want {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return fmt.Errorf("stream ended after %d of %d declared bytes", written+n, realSize)
				}
				return err
			}
			if written+want == realSize {
				// Just consumed the last real byte: reject an over-long
				// stream now, same semantics as writeExact's tail check.
				extra := make([]byte, 1)
				en, eerr := io.ReadFull(r, extra)
				if en > 0 {
					return fmt.Errorf("stream carried more than declared total_size=%d", realSize)
				}
				if eerr != io.EOF {
					return fmt.Errorf("finish stream: %w", eerr)
				}
			}
		}
		if n < chunkLen {
			clear(buf[n:chunkLen])
		}
		if err := writeAllAt(f, buf[:chunkLen], int64(addr+written)); err != nil {
			return err
		}
		written += chunkLen
	}
	return nil
}

// alignedBuffer returns a size-byte slice whose backing address is a
// multiple of align, by over-allocating by align bytes and slicing to the
// first aligned offset. Mechanics copied from NBSS's directio.go as-is —
// unsafe.Pointer is only used to read a slice's backing address, never to
// write through it.
func alignedBuffer(size int, align int) []byte {
	if align <= 0 {
		return make([]byte, size)
	}
	raw := make([]byte, size+align)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	offset := int(addr % uintptr(align))
	if offset == 0 {
		return raw[:size]
	}
	start := align - offset
	return raw[start : start+size]
}

func writeAllAt(f io.WriterAt, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := f.WriteAt(b, off)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		off += int64(n)
		b = b[n:]
	}
	return nil
}
