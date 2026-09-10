package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/echo983/BSOS/internal/daemon/bsospb"
	"github.com/echo983/BSOS/internal/pan"
	"github.com/echo983/BSOS/internal/zram"
)

// Server implements docs/DESIGN.md §3.3's full two-phase write path
// across a multi-disk pool (§3.11): a pool-wide fid gate (step 0,
// spanning every disk) followed by best-fit disk selection and a
// per-disk extent reservation (step 1), an unlocked streaming write
// (step 2), and a commit or abort (steps 3/4).
type Server struct {
	bsospb.UnimplementedBSOSServer

	disks          []*DeviceState
	gate           *poolGate
	maxPut         uint64
	smallFileBytes uint64
	chdTargetP     float64

	stallTimeout    time.Duration
	gateFanoutLimit time.Duration
	degraded        bool

	stopCh       chan struct{}
	closeOnce    sync.Once
	trimInterval time.Duration

	grpcMu     sync.Mutex
	grpcServer *grpc.Server
}

func NewServer(cfg Config) (*Server, error) {
	if cfg.ReservationStallTimeout <= 0 || cfg.PoolGateFanoutTimeout <= 0 {
		return nil, fmt.Errorf("timeouts must be positive")
	}
	panFile, err := pan.Read(cfg.PanPath)
	if err != nil {
		return nil, fmt.Errorf("read pan: %w", err)
	}
	originalPan := panFile
	var warnings []error
	if cfg.ZramSnapshotDir != "" {
		panFile.Devices, warnings, err = zram.RestorePool(panFile.Devices, cfg.ZramSnapshotDir)
		if err != nil {
			return nil, fmt.Errorf("restore zram pool: %w", err)
		}
		for _, warning := range warnings {
			log.Printf("bsosd: degraded zram tier: %v", warning)
		}
	}
	if len(panFile.Devices) == 0 {
		return nil, fmt.Errorf("no devices in %s", cfg.PanPath)
	}

	var disks []*DeviceState
	opened := false
	defer func() {
		if !opened {
			for _, disk := range disks {
				_ = disk.Close()
			}
		}
	}()
	for _, dev := range panFile.Devices {

		diskID, err := pan.ParseDiskID(dev.DiskID)
		if err != nil {
			return nil, fmt.Errorf("parse disk id for %s: %w", dev.DevicePath, err)
		}
		disk, err := OpenDevice(dev.DevicePath, diskID, cfg.WriteDispatchConcurrency)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", dev.DevicePath, err)
		}
		disk.cfg = cfg
		for _, previous := range disks {
			if previous.diskID == disk.diskID {
				disk.Close()
				return nil, fmt.Errorf("duplicate disk id %d", disk.diskID)
			}
			for fid := range disk.confirmed {
				if _, exists := previous.confirmed[fid]; exists {
					disk.Close()
					return nil, fmt.Errorf("fid %d registered on multiple disks", fid)
				}
			}
		}
		disks = append(disks, disk)
	}
	if len(disks) == 0 {
		return nil, fmt.Errorf("no usable devices after opening %s", cfg.PanPath)
	}

	if cfg.ZramSnapshotDir != "" {
		if err := persistPoolMapping(cfg.PanPath, originalPan, panFile.Devices); err != nil {
			return nil, fmt.Errorf("persist restored pool: %w", err)
		}
	}
	opened = true
	srv := &Server{
		disks:           disks,
		degraded:        len(warnings) > 0,
		gate:            newPoolGate(),
		maxPut:          cfg.MaxPutBytes,
		smallFileBytes:  smallFileBytes(cfg.SmallFilePow2),
		chdTargetP:      cfg.ChdTargetP,
		stallTimeout:    cfg.ReservationStallTimeout,
		gateFanoutLimit: cfg.PoolGateFanoutTimeout,
		stopCh:          make(chan struct{}),
		trimInterval:    cfg.TrimInterval,
	}
	srv.startTrimScheduler(cfg.TrimInterval)
	return srv, nil
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
		s.grpcMu.Lock()
		gs := s.grpcServer
		s.grpcMu.Unlock()
		if gs != nil {
			gs.GracefulStop()
		}
	})
	var firstErr error
	for _, disk := range s.disks {
		if err := disk.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Server) Serve(listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}
	grpcServer := grpc.NewServer()
	bsospb.RegisterBSOSServer(grpcServer, s)
	s.grpcMu.Lock()
	s.grpcServer = grpcServer
	s.grpcMu.Unlock()
	log.Printf("bsosd: listening on %s (%d disk(s))", listenAddr, len(s.disks))
	return grpcServer.Serve(lis)
}

// confirmed is the pool-wide gate's confirmed-existence check
// (docs/DESIGN.md §3.3 step 0), bounded by its own fan-out timeout
// separate from the per-write stall timeout — one unresponsive disk
// must not freeze the whole pool's write path.
func (s *Server) confirmed(fid uint64) (bool, error) {
	type result struct {
		found bool
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		found, err := s.confirmedAnywhere(fid)
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
	if totalSize == 0 || aliasFor != 0 && (aliasFor == fid || totalSize < 2) {
		return status.Error(codes.InvalidArgument, "invalid size or alias")
	}
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
	defer s.gate.release(fid, aliasFor)

	// Step 1: best-fit disk selection (§3.11), then extent reservation.
	disk, err := s.selectDiskForWrite(totalSize)
	if err != nil {
		return status.Errorf(codes.ResourceExhausted, "select disk: %v", err)
	}
	pw, err := disk.PrepareWrite(fid, totalSize)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return status.Error(codes.AlreadyExists, "extent already reserved")
		}
		return status.Errorf(codes.Internal, "prepare: %v", err)
	}

	// Step 2: stream chunks in, unlocked, with a stall timeout.
	r := &stallingChunkReader{stream: stream, timeout: s.stallTimeout}
	writeErr := pw.WriteFromContext(stream.Context(), r)
	if writeErr != nil {
		pw.Abort() // Step 4.
		return status.Errorf(codes.Internal, "put: %v", writeErr)
	}

	// Step 3: commit. alias_for's jump-indicator pair must land on the
	// same disk as fid's real entry (docs/DESIGN.md §3.11) — Commit
	// writes both under pw's disk, which is exactly the one selected
	// above, so this is automatic, not something to coordinate further.
	if err := stream.Context().Err(); err != nil {
		pw.Abort()
		return status.FromContextError(err).Err()
	}
	if err := pw.Commit(aliasFor); err != nil {
		pw.Abort()
		return status.Errorf(codes.Internal, "commit: %v", err)
	}
	return stream.SendAndClose(&bsospb.PutResponse{})
}

func (s *Server) Get(req *bsospb.GetRequest, stream grpc.ServerStreamingServer[bsospb.GetResponse]) error {
	type found struct {
		disk *DeviceState
		ref  objectRef
		err  error
	}
	results := make(chan found, len(s.disks))
	for _, d := range s.disks {
		go func(d *DeviceState) { ref, err := d.lookup(req.GetFid()); results <- found{d, ref, err} }(d)
	}
	var selected found
	var firstErr error
	for range s.disks {
		select {
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case r := <-results:
			if r.err == nil {
				selected = r
			} else if !errors.Is(r.err, ErrNotFound) && firstErr == nil {
				firstErr = r.err
			}
		}
		if selected.disk != nil {
			break
		}
	}
	if selected.disk == nil {
		if firstErr != nil {
			return status.Errorf(codes.Internal, "get: %v", firstErr)
		}
		return status.Error(codes.NotFound, "not found")
	}
	size := selected.ref.size
	start, end := uint64(0), size
	if req.GetHasRange() {
		start, end = req.GetRangeStart(), req.GetRangeEnd()
		if end == 0 || end > size {
			end = size
		}
		if start > end {
			return status.Error(codes.InvalidArgument, "invalid range")
		}
	}
	// Bound both disk-read memory and each wire message. Range metadata describes
	// this message's byte interval within the logical object.
	for pos := start; ; {
		next := min(pos+uint64(1<<20), end)
		data, err := selected.disk.readRange(stream.Context(), selected.ref, pos, next)
		if err != nil {
			return status.Errorf(codes.Internal, "get: %v", err)
		}
		if err := stream.Send(&bsospb.GetResponse{Size: size, RangeStart: pos, RangeEnd: next, Data: data}); err != nil {
			return err
		}
		if next == end {
			return nil
		}
		pos = next
	}
}

func (s *Server) Head(_ context.Context, req *bsospb.HeadRequest) (*bsospb.HeadResponse, error) {
	size, err := s.headAny(req.GetFid())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, "not found")
		}
		return nil, status.Errorf(codes.Internal, "head: %v", err)
	}
	return &bsospb.HeadResponse{Size: size}, nil
}

func (s *Server) Health(_ context.Context, _ *bsospb.Empty) (*bsospb.HealthResponse, error) {
	healthy := len(s.disks) > 0 && !s.degraded
	for _, d := range s.disks {
		d.indexMu.RLock()
		indexFault := d.indexErr != nil
		d.indexMu.RUnlock()
		if indexFault || !s.canWrite(d, 1) {
			healthy = false
			break
		}
	}
	return &bsospb.HealthResponse{Ok: healthy}, nil
}

// DiskCount returns the number of disks registered in this pool.
func (s *Server) DiskCount() int {
	return len(s.disks)
}

// DiskIDs returns the diskIDs of all disks in the pool.
func (s *Server) DiskIDs() []uint64 {
	ids := make([]uint64, len(s.disks))
	for i, d := range s.disks {
		ids[i] = d.diskID
	}
	return ids
}

// Degraded reports whether the server is operating in degraded mode.
func (s *Server) Degraded() bool {
	return s.degraded
}

func (s *Server) startTrimScheduler(interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, disk := range s.disks {
					if !s.canWrite(disk, 1) {
						continue
					}
					if err := disk.maybeTrim(); err != nil {
						log.Printf("bsosd: trim failed disk_id=0x%X device=%s: %v", disk.diskID, disk.devicePath, err)
					}
				}
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *Server) bonnieCHDPow2() (int, bool) {
	var maxChd uint64
	for _, disk := range s.disks {
		if zram.IsZramDevicePath(disk.devicePath) {
			continue
		}
		if !s.canWrite(disk, 1) {
			continue
		}
		chd := disk.currentCHD(s.chdTargetP)
		if chd > maxChd {
			maxChd = chd
		}
	}
	if maxChd == 0 {
		return 0, false
	}
	capSize := maxChd
	if s.maxPut > 0 && capSize > s.maxPut {
		capSize = s.maxPut
	}
	return pow2Log(capSize), true
}

func (s *Server) Bonnie(_ context.Context, _ *bsospb.Empty) (*bsospb.BonnieResponse, error) {
	pow2, ok := s.bonnieCHDPow2()
	if !ok {
		return &bsospb.BonnieResponse{ChDPow2: 0}, nil
	}
	return &bsospb.BonnieResponse{ChDPow2: uint32(pow2)}, nil
}

// stallingChunkReader adapts a Put client-stream into an io.Reader,
// yielding the bytes of successive `chunk` messages, and enforces
// docs/DESIGN.md §3.3's reservation stall timeout: if no chunk arrives
// within the configured window, Read returns an error instead of
// blocking forever on a client that went silent mid-stream.
type stallingChunkReader struct {
	stream   grpc.ClientStreamingServer[bsospb.PutRequest, bsospb.PutResponse]
	timeout  time.Duration
	buf      []byte
	deadline time.Time
}

func (r *stallingChunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.deadline.IsZero() {
		r.deadline = time.Now().Add(r.timeout)
	}
	for len(r.buf) == 0 {
		req, err := r.recv()
		if err != nil {
			return 0, err
		}
		msg, ok := req.Msg.(*bsospb.PutRequest_Chunk)
		if !ok {
			return 0, fmt.Errorf("expected a chunk message, got another header")
		}
		r.buf = msg.Chunk
		if len(r.buf) > 0 {
			r.deadline = time.Now().Add(r.timeout)
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *stallingChunkReader) recv() (*bsospb.PutRequest, error) {
	if time.Until(r.deadline) <= 0 {
		return nil, fmt.Errorf("stall timeout: no forward progress within %s", r.timeout)
	}
	type result struct {
		req *bsospb.PutRequest
		err error
	}
	ch := make(chan result, 1)
	go func() {
		req, err := r.stream.Recv()
		ch <- result{req, err}
	}()
	timer := time.NewTimer(time.Until(r.deadline))
	defer timer.Stop()
	select {
	case <-r.stream.Context().Done():
		return nil, r.stream.Context().Err()
	case res := <-ch:
		return res.req, res.err
	case <-timer.C:
		return nil, fmt.Errorf("stall timeout: no forward progress within %s", r.timeout)
	}
}

var _ io.Reader = (*stallingChunkReader)(nil)
