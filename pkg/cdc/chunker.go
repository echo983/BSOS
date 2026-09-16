package cdc

import (
	"fmt"
	"io"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	"github.com/zeebo/xxh3"
)

const (
	// DefaultMinSize is 4 MiB (preserves NVMe 4KB Slot alignment and prevents tiny fragments)
	DefaultMinSize = 4 << 20
	// DefaultTargetSize is 16 MiB (optimal L3 CPU cache locality and network throughput)
	DefaultTargetSize = 16 << 20
	// DefaultMaxSize is 32 MiB (safely beneath server max_put limit of 256MB)
	DefaultMaxSize = 32 << 20
)

// Options specifies the FastCDC chunking parameters.
type Options struct {
	MinSize    int
	TargetSize int
	MaxSize    int
}

// DefaultOptions returns the standard BSOS FastCDC chunking options.
func DefaultOptions() Options {
	return Options{
		MinSize:    DefaultMinSize,
		TargetSize: DefaultTargetSize,
		MaxSize:    DefaultMaxSize,
	}
}

// Chunk represents a single content-defined slice of an object.
type Chunk struct {
	FID    uint64 // xxh3_64(Data)
	Offset uint64 // Byte offset within original source stream
	Size   uint64 // Length in bytes
	Data   []byte // Chunk payload
}

// Chunker wraps FastCDC algorithm to yield chunks from an io.Reader.
type Chunker struct {
	inner  *chunkers.Chunker
	offset uint64
}

// NewChunker creates a new FastCDC chunker reading from r.
func NewChunker(r io.Reader, opt ...Options) (*Chunker, error) {
	opts := DefaultOptions()
	if len(opt) > 0 {
		if opt[0].MinSize > 0 {
			opts.MinSize = opt[0].MinSize
		}
		if opt[0].TargetSize > 0 {
			opts.TargetSize = opt[0].TargetSize
		}
		if opt[0].MaxSize > 0 {
			opts.MaxSize = opt[0].MaxSize
		}
	}
	if opts.MinSize <= 0 || opts.TargetSize < opts.MinSize || opts.MaxSize < opts.TargetSize {
		return nil, fmt.Errorf("cdc: invalid chunker options: min=%d target=%d max=%d", opts.MinSize, opts.TargetSize, opts.MaxSize)
	}

	cOpts := &chunkers.ChunkerOpts{
		MinSize:    opts.MinSize,
		NormalSize: opts.TargetSize,
		MaxSize:    opts.MaxSize,
	}

	inner, err := chunkers.NewChunker("fastcdc", r, cOpts)
	if err != nil {
		return nil, fmt.Errorf("cdc: init fastcdc: %w", err)
	}

	return &Chunker{
		inner:  inner,
		offset: 0,
	}, nil
}

// Next returns the next content-defined Chunk from the stream.
// Returns io.EOF when the input reader is fully consumed.
func (c *Chunker) Next() (Chunk, error) {
	buf, err := c.inner.Next()
	if err != nil && err != io.EOF {
		return Chunk{}, err
	}
	if len(buf) == 0 {
		return Chunk{}, io.EOF
	}

	chunkSize := uint64(len(buf))
	chunkOffset := c.offset
	c.offset += chunkSize

	// Clone chunk buffer because underlying chunker reuses its buffer
	chunkData := make([]byte, chunkSize)
	copy(chunkData, buf)

	fid := xxh3.Hash(chunkData)

	return Chunk{
		FID:    fid,
		Offset: chunkOffset,
		Size:   chunkSize,
		Data:   chunkData,
	}, nil
}
