// Package client provides the official Go client library for BSOS
// (Bare Space Object Storage), implementing docs/CLIENT_SPEC.md in full.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"bsos/internal/daemon/bsospb"
)

const (
	// DefaultChunkSize is 1 MiB per message, matching the maximum payload
	// chunk size recommended and implemented by BSOS daemons.
	DefaultChunkSize = 1 << 20

	// MaxJumpCode is the upper bound of the 1-byte jump code space (1..255).
	MaxJumpCode = 255
)

// ErrJumpExhausted is returned when all jump codes in 1..255 (or configured limit)
// produce a conflict.
var ErrJumpExhausted = errors.New("jump retry exhausted: all jump codes collided")

// Client interacts with a BSOS daemon over gRPC.
type Client struct {
	conn      *grpc.ClientConn
	pb        bsospb.BSOSClient
	chunkSize int
	ownsConn  bool
}

// Option configures a Client.
type Option func(*clientConfig)

type clientConfig struct {
	chunkSize   int
	dialOptions []grpc.DialOption
}

// WithChunkSize sets the chunk size for streaming Put operations.
func WithChunkSize(size int) Option {
	return func(cfg *clientConfig) {
		if size > 0 {
			cfg.chunkSize = size
		}
	}
}

// WithDialOptions appends custom gRPC dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(cfg *clientConfig) {
		cfg.dialOptions = append(cfg.dialOptions, opts...)
	}
}

// New connects to the BSOS daemon at target (e.g. "127.0.0.1:9090").
// Insecure credentials are used by default unless overridden via WithDialOptions.
func New(target string, opts ...Option) (*Client, error) {
	cfg := clientConfig{
		chunkSize: DefaultChunkSize,
		dialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	cc, err := grpc.NewClient(target, cfg.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("bsos client: dial %s: %w", target, err)
	}

	return &Client{
		conn:      cc,
		pb:        bsospb.NewBSOSClient(cc),
		chunkSize: cfg.chunkSize,
		ownsConn:  true,
	}, nil
}

// NewFromConn wraps an existing gRPC client connection.
func NewFromConn(cc *grpc.ClientConn, opts ...Option) *Client {
	cfg := clientConfig{
		chunkSize: DefaultChunkSize,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Client{
		conn:      cc,
		pb:        bsospb.NewBSOSClient(cc),
		chunkSize: cfg.chunkSize,
		ownsConn:  false,
	}
}

// Close closes the underlying gRPC connection if created by New.
func (c *Client) Close() error {
	if c.ownsConn && c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ComputeFID computes the 64-bit FID for a payload using xxh3_64,
// per docs/CLIENT_SPEC.md §1.
func ComputeFID(content []byte) uint64 {
	return xxh3.Hash(content)
}

// Put streams payload from r to the BSOS daemon per docs/CLIENT_SPEC.md §2.
//
// Protocol requirements:
//  1. Sends PutHeader as the first message on the stream.
//  2. Streams chunks totaling exactly totalSize bytes.
//  3. Checks for stream/send errors after EVERY chunk send and immediately stops
//     sending if the server rejects early (e.g. conflict, bad size, unroutable).
//  4. Surfaces gRPC status errors (e.g. AlreadyExists on conflict).
func (c *Client) Put(ctx context.Context, fid uint64, totalSize uint64, aliasFor uint64, r io.Reader) error {
	stream, err := c.pb.Put(ctx)
	if err != nil {
		return err
	}

	// Step 1: Send PutHeader as the first message
	hdrReq := &bsospb.PutRequest{
		Msg: &bsospb.PutRequest_Header{
			Header: &bsospb.PutHeader{
				Fid:       fid,
				TotalSize: totalSize,
				AliasFor:  aliasFor,
			},
		},
	}
	if err := stream.Send(hdrReq); err != nil {
		_, recvErr := stream.CloseAndRecv()
		if recvErr != nil {
			return recvErr
		}
		return err
	}

	// Step 2 & 3: Stream payload in chunks, checking for send errors after every chunk
	buf := make([]byte, c.chunkSize)
	var totalSent uint64

	for totalSent < totalSize {
		toRead := int(min(uint64(len(buf)), totalSize-totalSent))
		n, readErr := io.ReadFull(r, buf[:toRead])
		if n > 0 {
			chunkReq := &bsospb.PutRequest{
				Msg: &bsospb.PutRequest_Chunk{
					Chunk: buf[:n],
				},
			}
			if sendErr := stream.Send(chunkReq); sendErr != nil {
				// docs/CLIENT_SPEC.md §2.3: Check for a stream/send error after every chunk send,
				// and stop sending immediately if one occurs.
				_, recvErr := stream.CloseAndRecv()
				if recvErr != nil {
					return recvErr
				}
				return sendErr
			}
			totalSent += uint64(n)
		}

		if readErr != nil {
			if readErr == io.EOF || errors.Is(readErr, io.ErrUnexpectedEOF) {
				if totalSent < totalSize {
					_ = stream.CloseSend()
					return fmt.Errorf("premature EOF from source: read %d bytes, declared %d: %w", totalSent, totalSize, readErr)
				}
				break
			}
			_ = stream.CloseSend()
			return fmt.Errorf("read error: %w", readErr)
		}
	}

	if totalSent != totalSize {
		_ = stream.CloseSend()
		return fmt.Errorf("payload size mismatch: sent %d bytes, expected %d", totalSent, totalSize)
	}

	_, err = stream.CloseAndRecv()
	return err
}

// PutBytes is a convenience method that computes fid = ComputeFID(data)
// and performs a standard direct write.
func (c *Client) PutBytes(ctx context.Context, data []byte) (uint64, error) {
	fid := ComputeFID(data)
	err := c.Put(ctx, fid, uint64(len(data)), 0, bytes.NewReader(data))
	return fid, err
}

// JumpResult describes the outcome of PutWithJumpRetry.
type JumpResult struct {
	// FID is the primary logical content identifier (xxh3_64(originalContent)).
	FID uint64
	// TargetFID is the physical entry identifier where data was written.
	// Equal to FID if written directly, or the jump hash (xxh3_64(content + byte(jumpCode)))
	// if resolved via a one-hop alias jump pointer.
	TargetFID uint64
	// JumpsTaken is 0 for direct placement, or 1..255 indicating the jump code used.
	JumpsTaken int
}

// JumpOptions configures collision retry behavior.
type JumpOptions struct {
	// MaxJumps limits how many jump codes to attempt (1..255, default 255).
	MaxJumps int
}

// PutWithJumpRetry implements docs/CLIENT_SPEC.md §3 collision handling:
//  1. Attempts direct Put of content under fid = xxh3_64(content).
//  2. On conflict (AlreadyExists), iterates jumpCode in 1..MaxJumps:
//     jumpData = content + byte(jumpCode)
//     jumpFID  = xxh3_64(jumpData)
//     try Put(header{fid: jumpFID, total_size: len(jumpData), alias_for: fid})
//  3. On success, data is accessible via both logical fid (with suffix stripped)
//     and physical jumpFID.
func (c *Client) PutWithJumpRetry(ctx context.Context, data []byte, opts ...JumpOptions) (JumpResult, error) {
	maxJumps := MaxJumpCode
	if len(opts) > 0 && opts[0].MaxJumps > 0 {
		maxJumps = min(opts[0].MaxJumps, MaxJumpCode)
	}

	fid := ComputeFID(data)
	err := c.Put(ctx, fid, uint64(len(data)), 0, bytes.NewReader(data))
	if err == nil {
		return JumpResult{
			FID:        fid,
			TargetFID:  fid,
			JumpsTaken: 0,
		}, nil
	}

	if !IsConflict(err) {
		return JumpResult{FID: fid}, err
	}

	// Collision occurred: run the one-hop jump-retry loop
	for jumpCode := 1; jumpCode <= maxJumps; jumpCode++ {
		jumpData := make([]byte, len(data)+1)
		copy(jumpData, data)
		jumpData[len(data)] = byte(jumpCode)
		jumpFID := ComputeFID(jumpData)

		err := c.Put(ctx, jumpFID, uint64(len(jumpData)), fid, bytes.NewReader(jumpData))
		if err == nil {
			return JumpResult{
				FID:        fid,
				TargetFID:  jumpFID,
				JumpsTaken: jumpCode,
			}, nil
		}
		if !IsConflict(err) {
			return JumpResult{FID: fid, TargetFID: jumpFID, JumpsTaken: jumpCode}, err
		}
	}

	return JumpResult{FID: fid, JumpsTaken: maxJumps}, ErrJumpExhausted
}

// Range specifies a byte interval [Start, End) for Get requests.
// If End is 0, reading continues to the end of the object.
type Range struct {
	Start uint64
	End   uint64
}

// Get streams the object content for fid into w.
// If optRange is specified, only the requested byte slice is retrieved.
// Returns total logical object size, bytes written to w, and any error.
func (c *Client) Get(ctx context.Context, fid uint64, w io.Writer, optRange ...Range) (uint64, uint64, error) {
	req := &bsospb.GetRequest{
		Fid: fid,
	}
	if len(optRange) > 0 {
		req.HasRange = true
		req.RangeStart = optRange[0].Start
		req.RangeEnd = optRange[0].End
	}

	stream, err := c.pb.Get(ctx, req)
	if err != nil {
		return 0, 0, err
	}

	var totalSize uint64
	var written uint64

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, written, err
		}

		totalSize = resp.GetSize()
		if len(resp.GetData()) > 0 {
			n, writeErr := w.Write(resp.GetData())
			written += uint64(n)
			if writeErr != nil {
				return totalSize, written, writeErr
			}
		}
	}

	return totalSize, written, nil
}

// GetBytes retrieves the object (or range) as a byte slice.
func (c *Client) GetBytes(ctx context.Context, fid uint64, optRange ...Range) ([]byte, error) {
	var buf bytes.Buffer
	_, _, err := c.Get(ctx, fid, &buf, optRange...)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Head returns the logical size of an object without retrieving its payload.
func (c *Client) Head(ctx context.Context, fid uint64) (uint64, error) {
	resp, err := c.pb.Head(ctx, &bsospb.HeadRequest{Fid: fid})
	if err != nil {
		return 0, err
	}
	return resp.GetSize(), nil
}

// Bonnie returns Bonnie's ch_d_pow2 metric, representing the largest object
// size (2^ch_d_pow2 bytes) placeable with the pool's configured target probability.
func (c *Client) Bonnie(ctx context.Context) (uint32, error) {
	resp, err := c.pb.Bonnie(ctx, &bsospb.Empty{})
	if err != nil {
		return 0, err
	}
	return resp.GetChDPow2(), nil
}

// Health checks daemon and pool health.
func (c *Client) Health(ctx context.Context) (bool, error) {
	resp, err := c.pb.Health(ctx, &bsospb.Empty{})
	if err != nil {
		return false, err
	}
	return resp.GetOk(), nil
}

// VerifyOptions configures VerifyContent retry-safety verification.
type VerifyOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	CompareContent bool
}

// VerifyContent implements docs/CLIENT_SPEC.md §4 retry-safety verification:
// When a Put receives a conflict on retry, this helper reads back the fid
// (handling transient NotFound while another write is in flight) and verifies
// either payload match or size equality.
func (c *Client) VerifyContent(ctx context.Context, fid uint64, expected []byte, opts ...VerifyOptions) (bool, error) {
	cfg := VerifyOptions{
		MaxAttempts:    10,
		InitialBackoff: 20 * time.Millisecond,
		CompareContent: true,
	}
	if len(opts) > 0 {
		if opts[0].MaxAttempts > 0 {
			cfg.MaxAttempts = opts[0].MaxAttempts
		}
		if opts[0].InitialBackoff > 0 {
			cfg.InitialBackoff = opts[0].InitialBackoff
		}
		cfg.CompareContent = opts[0].CompareContent
	}

	backoff := cfg.InitialBackoff
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(backoff):
				backoff = min(backoff*3/2, 500*time.Millisecond)
			}
		}

		if cfg.CompareContent {
			got, err := c.GetBytes(ctx, fid)
			if err != nil {
				if IsNotFound(err) {
					continue
				}
				return false, err
			}
			return bytes.Equal(got, expected), nil
		}

		size, err := c.Head(ctx, fid)
		if err != nil {
			if IsNotFound(err) {
				continue
			}
			return false, err
		}
		return size == uint64(len(expected)), nil
	}

	return false, fmt.Errorf("verify fid 0x%X: still not found after %d attempts", fid, cfg.MaxAttempts)
}

// IsConflict returns true if err indicates a conflict (codes.AlreadyExists).
func IsConflict(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

// IsNotFound returns true if err indicates the object was not found (codes.NotFound).
func IsNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

// IsResourceExhausted returns true if err is codes.ResourceExhausted.
func IsResourceExhausted(err error) bool {
	return status.Code(err) == codes.ResourceExhausted
}

// IsInvalidArgument returns true if err is codes.InvalidArgument.
func IsInvalidArgument(err error) bool {
	return status.Code(err) == codes.InvalidArgument
}
