package zram

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"bsos/internal/blk"
)

type createOptions struct {
	device string
	note   string
	id     string
}

func RunCreate(args []string) int {
	opts := createOptions{}
	fs := flag.NewFlagSet("bsos zram create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.device, "device", "", "")
	fs.StringVar(&opts.note, "note", "", "")
	fs.StringVar(&opts.id, "id", "auto", "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return exitBadArgs
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: size required")
		return exitBadArgs
	}

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for zram access")
		return exitPerm
	}

	size, err := parseSize(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_SIZE: %v\n", err)
		return exitBadArgs
	}

	devicePath, err := resolveZramDevice(opts.device)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_DEVICE: %v\n", err)
		return exitBadArgs
	}
	if devicePath == "" {
		alloc, err := newZramAllocator()
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_ZRAM_ALLOC: %v\n", err)
			return exitIO
		}
		if !alloc.canHotAdd && len(alloc.free) == 0 {
			fmt.Fprintln(os.Stderr, "E_ZRAM_ALLOC: no empty zram devices and hot_add unavailable; try `modprobe zram num_devices=N` or pass --device")
			return exitIO
		}
		devicePath, err = alloc.next()
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_ZRAM_ALLOC: %v\n", err)
			return exitIO
		}
	}

	if existing, err := readZramDiskSize(devicePath); err != nil {
		fmt.Fprintf(os.Stderr, "E_ZRAM_SIZE: %v\n", err)
		return exitIO
	} else if existing > 0 {
		fmt.Fprintf(os.Stderr, "E_ZRAM_IN_USE: %s size=%d\n", devicePath, existing)
		return exitIO
	}
	if err := writeZramDiskSize(devicePath, size); err != nil {
		fmt.Fprintf(os.Stderr, "E_ZRAM_SET_SIZE: %v\n", err)
		return exitIO
	}

	initArgs := []string{"--yes", "--id", opts.id}
	if strings.TrimSpace(opts.note) != "" {
		initArgs = append(initArgs, "--note", opts.note)
	}
	initArgs = append(initArgs, devicePath)
	if code := blk.RunInit(initArgs); code != 0 {
		return code
	}

	fmt.Printf("OK: zram device initialized %s size=%d\n", devicePath, size)
	return exitOK
}

func addZramDevice() (string, error) {
	if _, err := os.Stat("/sys/class/zram-control/hot_add"); err != nil {
		return "", fmt.Errorf("zram-control not available; load zram module (modprobe zram)")
	}
	if !hotAddAvailable() {
		return "", fmt.Errorf("zram hot_add unavailable; create devices with modprobe zram num_devices=N or use --device")
	}
	// Reading hot_add creates a device and returns its numeric id.
	// https://docs.kernel.org/admin-guide/blockdev/zram.html
	raw, err := os.ReadFile("/sys/class/zram-control/hot_add")
	if err != nil {
		return "", err
	}
	id, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 32)
	if err != nil {
		return "", fmt.Errorf("invalid hot_add response: %w", err)
	}
	return fmt.Sprintf("/dev/zram%d", id), nil

}

func findUnusedZramDevice() (string, error) {
	devices, err := listUnusedZramDevices()
	if err != nil {
		return "", err
	}
	if len(devices) == 0 {
		return "", nil
	}
	return devices[0], nil
}

func listZramDevices() (map[string]bool, error) {
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "zram") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}
	return out, nil
}
