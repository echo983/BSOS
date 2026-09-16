package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/zeebo/xxh3"

	"github.com/echo983/BSOS/pkg/bsospb"
	"github.com/echo983/BSOS/pkg/cdc"
	"github.com/echo983/BSOS/pkg/manifest"
)

const (
	// DefaultCDCWorkers is the default number of concurrent upload worker goroutines.
	DefaultCDCWorkers = 8
)

// CDCOptions configures CDC chunking and parallel upload.
type CDCOptions struct {
	Workers          int
	MinChunkSize     int
	TargetChunkSize  int
	MaxChunkSize     int
	Filename         string
	ProgressCallback func(completedChunks, totalChunks int, bytesUploaded uint64)
}

// CDCResult contains the outcome of a chunked CDC upload.
type CDCResult struct {
	ManifestFID       uint64
	TotalSize         uint64
	FullContentHash   uint64
	ChunkCount        int
	DeduplicatedCount int
	UploadedCount     int
	Chunks            []*bsospb.ChunkDescriptor
}

// PutFileCDC uploads a local file using FastCDC dynamic chunking and parallel multi-worker upload.
func (c *Client) PutFileCDC(ctx context.Context, localPath string, opt ...CDCOptions) (CDCResult, error) {
	stat, err := os.Stat(localPath)
	if err != nil {
		return CDCResult{}, fmt.Errorf("bsos client: stat file %s: %w", localPath, err)
	}
	if stat.IsDir() {
		return CDCResult{}, fmt.Errorf("bsos client: %s is a directory", localPath)
	}
	size := uint64(stat.Size())
	if size == 0 {
		return CDCResult{}, fmt.Errorf("bsos client: empty file %s not allowed", localPath)
	}

	f, err := os.Open(localPath)
	if err != nil {
		return CDCResult{}, fmt.Errorf("bsos client: open file %s: %w", localPath, err)
	}
	defer f.Close()

	opts := CDCOptions{}
	if len(opt) > 0 {
		opts = opt[0]
	}
	if opts.Filename == "" {
		opts.Filename = stat.Name()
	}

	return c.PutCDC(ctx, f, size, opts)
}

type chunkTask struct {
	index int
	chunk cdc.Chunk
}

type chunkResult struct {
	index int
	desc  *bsospb.ChunkDescriptor
	isDup bool
	err   error
}

// PutCDC reads data from r, slices it into content-defined chunks with FastCDC, uploads chunks
// concurrently with a worker pool, and commits the FileManifest root object.
func (c *Client) PutCDC(ctx context.Context, r io.Reader, expectedSize uint64, opt ...CDCOptions) (CDCResult, error) {
	opts := CDCOptions{
		Workers:         DefaultCDCWorkers,
		MinChunkSize:    cdc.DefaultMinSize,
		TargetChunkSize: cdc.DefaultTargetSize,
		MaxChunkSize:    cdc.DefaultMaxSize,
	}
	if len(opt) > 0 {
		if opt[0].Workers > 0 {
			opts.Workers = opt[0].Workers
		}
		if opt[0].MinChunkSize > 0 {
			opts.MinChunkSize = opt[0].MinChunkSize
		}
		if opt[0].TargetChunkSize > 0 {
			opts.TargetChunkSize = opt[0].TargetChunkSize
		}
		if opt[0].MaxChunkSize > 0 {
			opts.MaxChunkSize = opt[0].MaxChunkSize
		}
		if opt[0].Filename != "" {
			opts.Filename = opt[0].Filename
		}
		opts.ProgressCallback = opt[0].ProgressCallback
	}

	chunkerOpts := cdc.Options{
		MinSize:    opts.MinChunkSize,
		TargetSize: opts.TargetChunkSize,
		MaxSize:    opts.MaxChunkSize,
	}

	chunker, err := cdc.NewChunker(r, chunkerOpts)
	if err != nil {
		return CDCResult{}, fmt.Errorf("bsos client: init chunker: %w", err)
	}

	fullHasher := xxh3.New()
	taskCh := make(chan chunkTask, opts.Workers*2)
	resCh := make(chan chunkResult, opts.Workers*2)

	var wg sync.WaitGroup
	ctxWorker, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	// Launch upload workers
	for w := 0; w < opts.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				select {
				case <-ctxWorker.Done():
					return
				default:
				}

				res, putErr := c.PutWithJumpRetry(ctxWorker, task.chunk.Data)
				if putErr != nil {
					select {
					case resCh <- chunkResult{index: task.index, err: putErr}:
					case <-ctxWorker.Done():
					}
					return
				}

				desc := &bsospb.ChunkDescriptor{
					Fid:    task.chunk.FID,
					Offset: task.chunk.Offset,
					Size:   task.chunk.Size,
				}
				select {
				case resCh <- chunkResult{index: task.index, desc: desc, isDup: res.IsDuplicate}:
				case <-ctxWorker.Done():
					return
				}
			}
		}()
	}

	// Closer goroutine: closes resCh after all workers finish
	go func() {
		wg.Wait()
		close(resCh)
	}()

	// Slicer loop running in separate goroutine
	var sliceErr error
	go func() {
		defer close(taskCh)
		chunkIndex := 0
		for {
			ch, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				sliceErr = err
				cancelWorkers()
				break
			}

			// Update full content hash
			fullHasher.Write(ch.Data)

			task := chunkTask{index: chunkIndex, chunk: ch}
			chunkIndex++

			select {
			case taskCh <- task:
			case <-ctxWorker.Done():
				return
			}
		}
	}()

	// Collector loop
	results := make([]*bsospb.ChunkDescriptor, 0)
	var dedupCount, uploadCount int
	var firstErr error

	for res := range resCh {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancelWorkers()
		}
		if res.desc != nil {
			results = append(results, res.desc)
			if res.isDup {
				dedupCount++
			} else {
				uploadCount++
			}
		}
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(len(results), len(results), 0)
		}
	}

	if sliceErr != nil {
		return CDCResult{}, fmt.Errorf("bsos client: cdc slice error: %w", sliceErr)
	}
	if firstErr != nil {
		return CDCResult{}, fmt.Errorf("bsos client: cdc chunk upload error: %w", firstErr)
	}

	// Sort chunk descriptors by offset
	sort.Slice(results, func(i, j int) bool {
		return results[i].Offset < results[j].Offset
	})

	var actualTotalSize uint64
	for _, desc := range results {
		actualTotalSize += desc.Size
	}

	fullContentHash := fullHasher.Sum64()

	// Assemble FileManifest
	fileManifest := &bsospb.FileManifest{
		Version:         manifest.CurrentVersion,
		TotalSize:       actualTotalSize,
		FullContentHash: fullContentHash,
		Filename:        opts.Filename,
		ChunkTargetSize: uint64(opts.TargetChunkSize),
		Chunks:          results,
	}

	manifestBytes, err := manifest.Encode(fileManifest)
	if err != nil {
		return CDCResult{}, fmt.Errorf("bsos client: encode manifest: %w", err)
	}

	// Commit Manifest as atomic object
	manifestRes, err := c.PutAtomic(ctx, manifestBytes)
	if err != nil {
		return CDCResult{}, fmt.Errorf("bsos client: commit manifest object: %w", err)
	}

	return CDCResult{
		ManifestFID:       manifestRes.FID,
		TotalSize:         actualTotalSize,
		FullContentHash:   fullContentHash,
		ChunkCount:        len(results),
		DeduplicatedCount: dedupCount,
		UploadedCount:     uploadCount,
		Chunks:            results,
	}, nil
}

// InspectManifest reads and decodes the FileManifest stored under manifestFID.
func (c *Client) InspectManifest(ctx context.Context, manifestFID uint64) (*bsospb.FileManifest, error) {
	data, err := c.GetBytes(ctx, manifestFID)
	if err != nil {
		return nil, fmt.Errorf("bsos client: fetch manifest: %w", err)
	}
	return manifest.Decode(data)
}

// GetCDC downloads and reassembles a chunked file represented by manifestFID.
// If optRange is specified, only chunks overlapping the range are downloaded (sparse Range read).
func (c *Client) GetCDC(ctx context.Context, manifestFID uint64, w io.Writer, optRange ...Range) error {
	m, err := c.InspectManifest(ctx, manifestFID)
	if err != nil {
		return err
	}

	hasRange := len(optRange) > 0
	var rangeStart, rangeEnd uint64
	if hasRange {
		rangeStart = optRange[0].Start
		rangeEnd = optRange[0].End
		if rangeEnd == 0 || rangeEnd > m.TotalSize {
			rangeEnd = m.TotalSize
		}
		if rangeStart >= rangeEnd {
			return nil // Zero-length range
		}
	} else {
		rangeStart = 0
		rangeEnd = m.TotalSize
	}

	// Stream overlapping chunks
	for _, ch := range m.Chunks {
		chunkStart := ch.Offset
		chunkEnd := ch.Offset + ch.Size

		// Check overlap with [rangeStart, rangeEnd)
		if chunkEnd <= rangeStart || chunkStart >= rangeEnd {
			continue // Skip non-overlapping chunk completely
		}

		// Download chunk data by logical FID (daemon automatically resolves alias jumps)
		chunkData, err := c.GetBytes(ctx, ch.Fid)
		if err != nil {
			return fmt.Errorf("bsos client: fetch chunk 0x%016X: %w", ch.Fid, err)
		}

		// Calculate window within this chunk
		sliceStart := uint64(0)
		if rangeStart > chunkStart {
			sliceStart = rangeStart - chunkStart
		}
		sliceEnd := ch.Size
		if rangeEnd < chunkEnd {
			sliceEnd = rangeEnd - chunkStart
		}

		if sliceStart < sliceEnd && sliceEnd <= uint64(len(chunkData)) {
			if _, err := w.Write(chunkData[sliceStart:sliceEnd]); err != nil {
				return fmt.Errorf("bsos client: write chunk output: %w", err)
			}
		}
	}

	return nil
}

// GetAuto automatically inspects whether fid is a FileManifest (starting with BSMN\x01)
// or a raw atomic object, transparently reassembling or streaming as appropriate.
func (c *Client) GetAuto(ctx context.Context, fid uint64, w io.Writer, optRange ...Range) error {
	// Probe the first 5 bytes
	probe, err := c.GetBytes(ctx, fid, Range{Start: 0, End: 5})
	if err == nil && manifest.IsManifest(probe) {
		return c.GetCDC(ctx, fid, w, optRange...)
	}

	// Not a manifest: download directly as raw atomic object
	_, _, err = c.Get(ctx, fid, w, optRange...)
	return err
}
