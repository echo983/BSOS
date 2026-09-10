package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"bsos/internal/daemon/bsospb"
	"bsos/internal/pan"
)

// Server implements docs/DESIGN.md §3.3's full two-phase write path:
// a pool-wide fid gate (step 0) followed by a per-disk extent
// reservation (step 1, DeviceState.PrepareWrite), an unlocked streaming
// write (step 2), and a commit or abort (steps 3/4). Milestone 4 (multi-
// disk pooling) is the only piece not yet here — this Server always
// targets a single disk, but the gate itself is already pool-scoped so
// multi-disk plugs in without redoing this milestone's work.
type Server struct {
	bsospb.UnimplementedBSOSServer

	disk   *DeviceState
	gate   *poolGate
	maxPut uint64

	stallTimeout    time.Duration
	gateFanoutLimit time.Duration
}

func NewServer(cfg Config) (*Server, error) {
	panFile, err := pan.Read(cfg.PanPath)
	if err != nil {
		return nil, fmt.Errorf("read pan: %w", err)
	}
	if len(panFile.Devices) == 0 {
		return nil, fmt.Errorf("no devices in %s", cfg.PanPath)
	}
	// Single-disk until milestone 4 brings in multidisk.go's best-fit
	// pool routing; the first device in pan.json is the only target.
	dev := panFile.Devices[0]
	diskID, err := pan.ParseDiskID(dev.DiskID)
	if err != nil {
		return nil, fmt.Errorf("parse disk id: %w", err)
	}
	disk, err := OpenDevice(dev.DevicePath, diskID, cfg.WriteDispatchConcurrency)
	if err != nil {
		return nil, err
	}
	return &Server{
		disk:            disk,
		gate:            newPoolGate(),
		maxPut:          cfg.MaxPutBytes,
		stallTimeout:    cfg.ReservationStallTimeout,
		gateFanoutLimit: cfg.PoolGateFanoutTimeout,
	}, nil
}

func (s *Server) Close() error {
	return s.disk.Close()
}

func (s *Server) Serve(listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}
	grpcServer := grpc.NewServer()
	bsospb.RegisterBSOSServer(grpcServer, s)
	log.Printf("bsosd: listening on %s (disk=%s)", listenAddr, s.disk.devicePath)
	return grpcServer.Serve(lis)
}

// confirmed is the pool-wide gate's confirmed-existence check
// (docs/DESIGN.md §3.3 step 0): today a single disk, degenerately
// "broadcast to everyone" until milestone 4; the timeout bound applies
// regardless of pool size, so it's already in place for when the fan-out
// becomes real.
func (s *Server) confirmed(fid uint64) (bool, error) {
	type result struct {
		found bool
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		found, err := s.disk.Confirmed(fid)
		ch <- result{found, err}
	}()
	select {
	case r := <-ch:
		return r.found, r.err
	case <-time.After(s.gateFanoutLimit):
		return false, fmt.Errorf("pool gate fan-out timed out after %s", s.gateFanoutLimit)
	}
}

func (s *Server) Put(stream grpc.ClientStreamingServer[bsospb.PutRequest, bsospb.PutResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first message must be PutHeader")
	}
	fid, totalSize, aliasFor := header.GetFid(), header.GetTotalSize(), header.GetAliasFor()
	if s.maxPut > 0 && totalSize > s.maxPut {
		return status.Error(codes.InvalidArgument, "payload exceeds max_put")
	}

	// Step 0: pool-wide fid gate, before disk selection.
	if err := s.gate.reserve(fid, aliasFor, s.confirmed); err != nil {
		if errors.Is(err, ErrConflict) {
			return status.Error(codes.AlreadyExists, "fid already registered")
		}
		return status.Errorf(codes.Internal, "pool gate: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			s.gate.release(fid, aliasFor)
		}
	}()

	// Step 1: disk selection (single-disk today) + extent reservation.
	pw, err := s.disk.PrepareWrite(fid, totalSize)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return status.Error(codes.AlreadyExists, "extent already reserved")
		}
		return status.Errorf(codes.Internal, "prepare: %v", err)
	}

	// Step 2: stream chunks in, unlocked, with a stall timeout.
	r := &stallingChunkReader{stream: stream, timeout: s.stallTimeout}
	writeErr := pw.WriteFrom(r)
	if writeErr != nil {
		pw.Abort() // Step 4.
		return status.Errorf(codes.Internal, "put: %v", writeErr)
	}

	// Step 3: commit.
	if err := pw.Commit(aliasFor); err != nil {
		pw.Abort()
		return status.Errorf(codes.Internal, "commit: %v", err)
	}
	committed = true
	return stream.SendAndClose(&bsospb.PutResponse{})
}

func (s *Server) Get(req *bsospb.GetRequest, stream grpc.ServerStreamingServer[bsospb.GetResponse]) error {
	data, size, err := s.disk.Get(req.GetFid())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return status.Error(codes.NotFound, "not found")
		}
		return status.Errorf(codes.Internal, "get: %v", err)
	}

	start, end := uint64(0), size
	if req.GetHasRange() {
		start, end = req.GetRangeStart(), req.GetRangeEnd()
		if end == 0 || end > size {
			end = size
		}
		if start > end {
			return status.Error(codes.InvalidArgument, "invalid range")
		}
		data = data[start:end]
	}

	// Milestone 2/3 simplification, not a correctness gap: sends the
	// whole (possibly range-limited) payload as one message rather than
	// chunking large objects across multiple GetResponse messages.
	return stream.Send(&bsospb.GetResponse{
		Size:       size,
		RangeStart: start,
		RangeEnd:   end,
		Data:       data,
	})
}

func (s *Server) Head(_ context.Context, req *bsospb.HeadRequest) (*bsospb.HeadResponse, error) {
	size, err := s.disk.Head(req.GetFid())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, "not found")
		}
		return nil, status.Errorf(codes.Internal, "head: %v", err)
	}
	return &bsospb.HeadResponse{Size: size}, nil
}

func (s *Server) Health(_ context.Context, _ *bsospb.Empty) (*bsospb.HealthResponse, error) {
	return &bsospb.HealthResponse{Ok: true}, nil
}

// stallingChunkReader adapts a Put client-stream into an io.Reader,
// yielding the bytes of successive `chunk` messages, and enforces
// docs/DESIGN.md §3.3's reservation stall timeout: if no chunk arrives
// within the configured window, Read returns an error instead of
// blocking forever on a client that went silent mid-stream.
type stallingChunkReader struct {
	stream  grpc.ClientStreamingServer[bsospb.PutRequest, bsospb.PutResponse]
	timeout time.Duration
	buf     []byte
}

func (r *stallingChunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		req, err := r.recv()
		if err != nil {
			return 0, err
		}
		chunk := req.GetChunk()
		if chunk == nil {
			return 0, fmt.Errorf("expected a chunk message, got another header")
		}
		r.buf = chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *stallingChunkReader) recv() (*bsospb.PutRequest, error) {
	type result struct {
		req *bsospb.PutRequest
		err error
	}
	ch := make(chan result, 1)
	go func() {
		req, err := r.stream.Recv()
		ch <- result{req, err}
	}()
	select {
	case res := <-ch:
		return res.req, res.err
	case <-time.After(r.timeout):
		return nil, fmt.Errorf("stall timeout: no forward progress within %s", r.timeout)
	}
}

var _ io.Reader = (*stallingChunkReader)(nil)
