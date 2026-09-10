package zram

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/echo983/BSOS/internal/pan"
)

type loadOptions struct {
	dir    string
	diskID string
}

func RunLoad(args []string) int {
	opts := loadOptions{}
	fs := flag.NewFlagSet("bsos zram load", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.dir, "dir", "", "")
	fs.StringVar(&opts.diskID, "disk", "", "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return exitBadArgs
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
		return exitBadArgs
	}

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for zram access")
		return exitPerm
	}

	dir := strings.TrimSpace(opts.dir)
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_HOME: %v\n", err)
			return exitIO
		}
		dir = filepath.Join(home, ".bsos_zram_snapshots")
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		fmt.Fprintf(os.Stderr, "E_DIR: snapshot dir not found: %s\n", dir)
		return exitNotFound
	}

	var targets []string
	if strings.TrimSpace(opts.diskID) != "" {
		name := pan.NormalizeID(opts.diskID)
		zstPath := filepath.Join(dir, name+".zst")
		if _, err := os.Stat(zstPath); err == nil {
			targets = append(targets, zstPath)
		} else {
			targets = append(targets, filepath.Join(dir, name))
		}
	} else {
		entries, err := os.ReadDir(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_DIR: %v\n", err)
			return exitIO
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasSuffix(name, ".sha256") || strings.HasSuffix(name, ".tmp") {
				continue
			}
			targets = append(targets, filepath.Join(dir, name))
		}
		sort.Strings(targets)
	}

	if len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "E_NO_SNAPSHOTS: no snapshot files found")
		return exitNotFound
	}

	alloc, err := newZramAllocator()
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_ZRAM_ALLOC: %v\n", err)
		return exitIO
	}
	if !alloc.canHotAdd && len(alloc.free) < len(targets) {
		fmt.Fprintf(os.Stderr, "E_ZRAM_ALLOC: only %d empty zram devices available; need %d (try `modprobe zram num_devices=%d` or free devices)\n", len(alloc.free), len(targets), len(targets))
		return exitIO
	}

	for _, path := range targets {
		if err := loadSnapshot(path, alloc); err != nil {
			fmt.Fprintf(os.Stderr, "E_LOAD_FAILED: %s %v\n", path, err)
			return exitIO
		}
	}

	fmt.Println("OK: load complete (run `bsos blk find` to refresh pan.json)")
	return exitOK
}

type zramAllocator struct {
	free      []string
	canHotAdd bool
}

func newZramAllocator() (*zramAllocator, error) {
	free, err := listUnusedZramDevices()
	if err != nil {
		return nil, err
	}
	return &zramAllocator{
		free:      free,
		canHotAdd: hotAddAvailable(),
	}, nil
}

func (a *zramAllocator) next() (string, error) {
	if len(a.free) > 0 {
		dev := a.free[0]
		a.free = a.free[1:]
		return dev, nil
	}
	if !a.canHotAdd {
		return "", fmt.Errorf("zram hot_add unavailable; create devices with modprobe zram num_devices=N")
	}
	return addZramDevice()
}

func loadSnapshot(path string, alloc *zramAllocator) error {
	id, err := pan.ParseDiskID(strings.TrimSuffix(filepath.Base(path), ".zst"))
	if err != nil {
		return err
	}
	snap, err := validateSnapshot(path, id)
	if err != nil {
		return err
	}
	device, err := alloc.next()
	if err != nil {
		return err
	}
	if err = (kernelRecovery{}).restore(device, snap); err != nil {
		return err
	}
	fmt.Printf("OK: loaded %s -> %s size=%d\n", filepath.Base(path), device, snap.size)
	return nil
}

func snapshotSize(path string) (uint64, error) {
	base := strings.TrimSuffix(filepath.Base(path), ".zst")
	metaPath := filepath.Join(filepath.Dir(path), base+".sha256")
	if size, _, err := readSnapshotMeta(metaPath); err == nil && size > 0 {
		return size, nil
	}
	if strings.HasSuffix(path, ".zst") {
		return 0, fmt.Errorf("missing snapshot meta for %s", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func loadRaw(devicePath, path string, size uint64) error {
	if err := writeZramDiskSize(devicePath, size); err != nil {
		return err
	}
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, 1024*1024)
	_, err = io.CopyBuffer(out, in, buf)
	return err
}

func loadCompressed(devicePath, path string, size uint64) error {
	if err := writeZramDiskSize(devicePath, size); err != nil {
		return err
	}
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()

	decoder, err := newZstdDecoder(in)
	if err != nil {
		return err
	}
	defer decoder.Close()

	out, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, 1024*1024)
	_, err = io.CopyBuffer(out, decoder, buf)
	return err
}
