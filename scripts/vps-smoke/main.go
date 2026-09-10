// vps-smoke checks the dedicated deployed test daemon over TCP.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/echo983/BSOS/internal/blk"
	"github.com/echo983/BSOS/internal/daemon/bsospb"
	"github.com/echo983/BSOS/internal/pan"
	"github.com/echo983/BSOS/internal/zram"
	"github.com/zeebo/xxh3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type object struct {
	Name string
	FID  uint64
	Size int
	Seed string
	Zram bool
}

func payload(o object) []byte {
	p := []byte(o.Seed + ":" + o.Name)
	return bytes.Repeat(p, o.Size/len(p)+1)[:o.Size]
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	mode := flag.String("mode", "verify", "seed or verify")
	address := flag.String("address", "127.0.0.1:19090", "test daemon")
	manifest := flag.String("manifest", "/var/lib/bsos-test/smoke.json", "fixture manifest")
	panPath := flag.String("pan", "/var/lib/bsos-test/pan.json", "pool mapping")
	flag.Parse()
	if *mode != "seed" && *mode != "verify" {
		return fmt.Errorf("invalid mode")
	}
	cc, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer cc.Close()
	c := bsospb.NewBSOSClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	h, err := c.Health(ctx, &bsospb.Empty{})
	if err != nil {
		return err
	}
	if !h.Ok {
		return fmt.Errorf("degraded health")
	}
	bonnie, err := c.Bonnie(ctx, &bsospb.Empty{})
	if err != nil {
		return fmt.Errorf("bonnie RPC: %w", err)
	}
	log.Printf("bonnie ch_d_pow2: %d", bonnie.GetChDPow2())
	var objects []object
	if *mode == "seed" {
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		objects = []object{{Name: "small", Size: 64 << 10, Seed: stamp, Zram: true}, {Name: "large", Size: 5 << 20, Seed: stamp}, {Name: "alias", Size: 32 << 10, Seed: stamp, Zram: true}}
		for i := range objects {
			o := &objects[i]
			data := payload(*o)
			o.FID = xxh3.Hash(data)
			fid, alias := o.FID, uint64(0)
			if o.Name == "alias" {
				alias = o.FID
				data = append(data, 1)
				fid = xxh3.Hash(data)
			}
			if err = put(ctx, c, fid, alias, uint64(len(data)), data); err != nil {
				return err
			}
		}
		raw, err := json.MarshalIndent(objects, "", "  ")
		if err != nil {
			return err
		}
		if err = os.WriteFile(*manifest, raw, 0600); err != nil {
			return err
		}
	} else {
		raw, err := os.ReadFile(*manifest)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &objects); err != nil {
			return err
		}
	}
	pool, err := pan.Read(*panPath)
	if err != nil {
		return err
	}
	for _, o := range objects {
		want := payload(o)
		data, messages, err := get(ctx, c, &bsospb.GetRequest{Fid: o.FID})
		if err != nil {
			return err
		}
		if !bytes.Equal(data, want) {
			return fmt.Errorf("%s content mismatch", o.Name)
		}
		head, err := c.Head(ctx, &bsospb.HeadRequest{Fid: o.FID})
		if err != nil || head.GetSize() != uint64(o.Size) {
			return fmt.Errorf("%s Head mismatch: %v", o.Name, err)
		}
		part, _, err := get(ctx, c, &bsospb.GetRequest{Fid: o.FID, HasRange: true, RangeStart: 7, RangeEnd: 77})
		if err != nil || !bytes.Equal(part, want[7:77]) {
			return fmt.Errorf("%s range mismatch: %v", o.Name, err)
		}
		count := 0
		for _, d := range pool.Devices {
			f, err := os.Open(d.DevicePath)
			if err != nil {
				return err
			}
			_, _, _, found, _, err := blk.FindLatestIndexEntry(f, o.FID)
			f.Close()
			if err != nil {
				return err
			}
			if found {
				count++
				if zram.IsZramDevicePath(d.DevicePath) != o.Zram {
					return fmt.Errorf("%s wrong tier", o.Name)
				}
			}
		}
		if count != 1 {
			return fmt.Errorf("%s registered on %d devices", o.Name, count)
		}
		fmt.Printf("verified %s bytes=%d messages=%d zram=%t sha256=%x\n", o.Name, len(data), messages, o.Zram, sha256.Sum256(data))
	}
	if *mode == "seed" {
		fid := xxh3.HashString("short:" + time.Now().String())
		if err := put(ctx, c, fid, 0, 10, []byte("abc")); err == nil {
			return fmt.Errorf("short stream accepted")
		}
		if _, err := c.Head(ctx, &bsospb.HeadRequest{Fid: fid}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("aborted object visible: %v", err)
		}
		if err := put(ctx, c, objects[0].FID, 0, uint64(objects[0].Size), nil); status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("duplicate accepted: %v", err)
		}
	}
	return nil
}
func put(ctx context.Context, c bsospb.BSOSClient, fid, alias, size uint64, data []byte) error {
	st, err := c.Put(ctx)
	if err != nil {
		return err
	}
	if err = st.Send(&bsospb.PutRequest{Msg: &bsospb.PutRequest_Header{Header: &bsospb.PutHeader{Fid: fid, AliasFor: alias, TotalSize: size}}}); err != nil {
		return err
	}
	for len(data) > 0 {
		n := min(len(data), 1<<20)
		if err = st.Send(&bsospb.PutRequest{Msg: &bsospb.PutRequest_Chunk{Chunk: data[:n]}}); err != nil {
			break
		}
		data = data[n:]
	}
	_, err = st.CloseAndRecv()
	return err
}
func get(ctx context.Context, c bsospb.BSOSClient, req *bsospb.GetRequest) ([]byte, int, error) {
	st, err := c.Get(ctx, req)
	if err != nil {
		return nil, 0, err
	}
	var data []byte
	messages := 0
	offset := uint64(0)
	if req.HasRange {
		offset = req.RangeStart
	}
	for {
		r, err := st.Recv()
		if err == io.EOF {
			return data, messages, nil
		}
		if err != nil {
			return nil, 0, err
		}
		if len(r.Data) > 1<<20 || r.RangeStart != offset || r.RangeEnd-r.RangeStart != uint64(len(r.Data)) {
			return nil, 0, fmt.Errorf("invalid response range or chunk size")
		}
		offset = r.RangeEnd
		messages++
		data = append(data, r.Data...)
	}
}
