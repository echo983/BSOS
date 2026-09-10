package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"bsos/internal/daemon/bsospb"
	"bsos/internal/pan"
)

// Server is milestone 2's single-disk gRPC server: correct wire
// protocol and write path, not yet the multi-disk pooling (§3.11) or
// two-phase concurrency model (§3.3) later milestones add.
type Server struct {
	bsospb.UnimplementedBSOSServer

	disk   *DeviceState
	maxPut uint64
}

func NewServer(cfg Config) (*Server, error) {
	panFile, err := pan.Read(cfg.PanPath)
	if err != nil {
		return nil, fmt.Errorf("read pan: %w", err)
	}
	if len(panFile.Devices) == 0 {
		return nil, fmt.Errorf("no devices in %s", cfg.PanPath)
	}
	// Milestone 2 is single-disk: the first device in pan.json. Milestone
	// 4 brings in multidisk.go's best-fit pool routing.
	dev := panFile.Devices[0]
	diskID, err := pan.ParseDiskID(dev.DiskID)
	if err != nil {
		return nil, fmt.Errorf("parse disk id: %w", err)
	}
	disk, err := OpenDevice(dev.DevicePath, diskID)
	if err != nil {
		return nil, err
	}
	return &Server{disk: disk, maxPut: cfg.MaxPutBytes}, nil
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

func (s *Server) Put(stream grpc.ClientStreamingServer[bsospb.PutRequest, bsospb.PutResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first message must be PutHeader")
	}
	if s.maxPut > 0 && header.GetTotalSize() > s.maxPut {
		return status.Error(codes.InvalidArgument, "payload exceeds max_put")
	}

	r := &chunkReader{stream: stream}
	err = s.disk.Put(header.GetFid(), header.GetTotalSize(), header.GetAliasFor(), r)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return status.Error(codes.AlreadyExists, "fid already registered")
		}
		return status.Errorf(codes.Internal, "put: %v", err)
	}
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

	// Milestone 2 sends the whole (possibly range-limited) payload as one
	// message; chunked responses for very large objects are a follow-up
	// refinement, not a wire-format change.
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

// chunkReader adapts a Put client-stream into an io.Reader, yielding the
// bytes of successive `chunk` messages after the leading PutHeader.
type chunkReader struct {
	stream grpc.ClientStreamingServer[bsospb.PutRequest, bsospb.PutResponse]
	buf    []byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		req, err := r.stream.Recv()
		if err != nil {
			return 0, err // io.EOF once the client calls CloseSend.
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

var _ io.Reader = (*chunkReader)(nil)
