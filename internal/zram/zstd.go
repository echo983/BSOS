package zram

import (
	"io"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

func newZstdEncoder(w io.Writer) (*zstd.Encoder, error) {
	concurrency := runtime.GOMAXPROCS(0)
	if concurrency < 1 {
		concurrency = 1
	}
	return zstd.NewWriter(w,
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
}

func newZstdDecoder(r io.Reader) (*zstd.Decoder, error) {
	concurrency := runtime.GOMAXPROCS(0)
	if concurrency < 1 {
		concurrency = 1
	}
	return zstd.NewReader(r, zstd.WithDecoderConcurrency(concurrency))
}
