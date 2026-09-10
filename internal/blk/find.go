package blk

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

const (
	findExitOK       = 0
	findExitFatal    = 1
	findExitNotFound = 2
	findExitBadArgs  = 3
	findExitPerm     = 4
)

type findOptions struct {
	all        bool
	include    string
	paths      arrayFlags
	jsonOutput bool
	quiet      bool
	showErrors bool
	rawPath    bool
}

type arrayFlags []string

func (a *arrayFlags) String() string {
	return strings.Join(*a, ",")
}

func (a *arrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

type findResult struct {
	Status     string `json:"status"`
	DevicePath string `json:"device_path"`
	SizeBytes  uint64 `json:"size_bytes,omitempty"`
	SizeGiB    string `json:"size_gib,omitempty"`
	Version    uint16 `json:"version,omitempty"`
	CapacityGB uint16 `json:"capacity_gb,omitempty"`
	DiskID     string `json:"disk_id,omitempty"`
	NoteBytes  int    `json:"note_bytes,omitempty"`
	GridStart  uint64 `json:"grid_start,omitempty"`
	IndexLen   uint64 `json:"index_len,omitempty"`
	Slots      uint64 `json:"slots,omitempty"`
	Error      string `json:"error,omitempty"`
}

type panFile struct {
	Version int          `json:"version"`
	Devices []findResult `json:"devices"`
}

func RunFind(args []string) int {
	opts := findOptions{}
	fs := flag.NewFlagSet("nbss blk find", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&opts.all, "all", false, "")
	fs.StringVar(&opts.include, "include", "", "")
	fs.Var(&opts.paths, "path", "")
	fs.BoolVar(&opts.jsonOutput, "json", false, "")
	fs.BoolVar(&opts.quiet, "quiet", false, "")
	fs.BoolVar(&opts.showErrors, "show-errors", false, "")
	fs.BoolVar(&opts.rawPath, "raw-path", false, "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return findExitBadArgs
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
		return findExitBadArgs
	}

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for block device access")
		return findExitPerm
	}

	devicePaths, err := resolveFindPaths(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_SCAN_FAILED: %v\n", err)
		return findExitFatal
	}

	matches := 0
	var matchResults []findResult
	if !opts.jsonOutput && !opts.quiet {
		fmt.Println("Found NBSS devices (magic=NBSS):")
	}

	var errorResults []findResult
	aliasMap := buildDeviceAliasMap("/dev/disk/by-id")
	for _, devPath := range devicePaths {
		result := scanDevice(devPath, aliasMap, opts.rawPath)
		switch result.Status {
		case "match":
			matches++
			matchResults = append(matchResults, result)
			printFindResult(result, opts)
		case "error":
			if opts.showErrors {
				errorResults = append(errorResults, result)
			}
		default:
		}
	}

	if opts.showErrors && len(errorResults) > 0 {
		for _, result := range errorResults {
			printFindResult(result, opts)
		}
	}

	if err := writePanJSON(matchResults); err != nil {
		fmt.Fprintf(os.Stderr, "E_WRITE_FAILED: %v\n", err)
		return findExitFatal
	}

	if matches > 0 {
		return findExitOK
	}
	return findExitNotFound
}

func resolveFindPaths(opts findOptions) ([]string, error) {
	if len(opts.paths) > 0 {
		paths := make([]string, 0, len(opts.paths))
		for _, p := range opts.paths {
			paths = append(paths, filepath.Clean(p))
		}
		return paths, nil
	}

	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, err
	}

	allow := buildAllowSet(opts)
	var paths []string
	for _, entry := range entries {
		name := entry.Name()
		if shouldIncludeDevice(name, opts, allow) {
			paths = append(paths, filepath.Join("/dev", name))
		}
	}
	return paths, nil
}

func buildAllowSet(opts findOptions) map[string]bool {
	allow := map[string]bool{
		"sd":     true,
		"nvme":   true,
		"mmcblk": true,
		"vd":     true,
		"zram":   true,
	}
	if opts.include == "" {
		return allow
	}
	for _, part := range strings.Split(opts.include, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		allow[part] = true
	}
	return allow
}

func shouldIncludeDevice(name string, opts findOptions, allow map[string]bool) bool {
	if opts.all {
		return true
	}
	if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") || strings.HasPrefix(name, "zram") {
		return allow[namePrefix(name)]
	}
	return allow[namePrefix(name)]
}

func namePrefix(name string) string {
	switch {
	case strings.HasPrefix(name, "nvme"):
		return "nvme"
	case strings.HasPrefix(name, "mmcblk"):
		return "mmcblk"
	case strings.HasPrefix(name, "sd"):
		return "sd"
	case strings.HasPrefix(name, "vd"):
		return "vd"
	case strings.HasPrefix(name, "dm-"):
		return "dm"
	case strings.HasPrefix(name, "loop"):
		return "loop"
	case strings.HasPrefix(name, "zram"):
		return "zram"
	case strings.HasPrefix(name, "ram"):
		return "ram"
	default:
		return name
	}
}

func scanDevice(devPath string, aliasMap map[uint64]string, rawPath bool) findResult {
	result := findResult{
		Status:     "not_nbss",
		DevicePath: devPath,
	}

	f, err := os.Open(devPath)
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
		return result
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
		return result
	}

	sizeBytes, err := deviceSizeBytes(f, st)
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
		return result
	}
	result.SizeBytes = sizeBytes
	result.SizeGiB = fmt.Sprintf("%.2f", float64(sizeBytes)/(1024*1024*1024))

	buf16 := make([]byte, 16)
	if _, err := f.ReadAt(buf16, 0); err != nil && err != io.EOF {
		result.Status = "error"
		result.Error = err.Error()
		return result
	}
	if string(buf16[:4]) != "NBSS" {
		return result
	}

	buf := make([]byte, HeaderBytes)
	if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
		result.Status = "error"
		result.Error = err.Error()
		return result
	}
	header, err := ParseHeader(buf)
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
		return result
	}
	if header.Version != FormatVersion {
		result.Status = "error"
		result.Error = fmt.Sprintf("unsupported version %d", header.Version)
		return result
	}

	result.Status = "match"
	result.Version = header.Version
	result.CapacityGB = header.CapacityGB
	result.DiskID = fmt.Sprintf("0x%X", header.DiskID)
	result.NoteBytes = header.NoteLen
	result.GridStart = GridStart
	result.IndexLen = IndexBytes
	if sizeBytes > GridStart {
		result.Slots = (sizeBytes - GridStart) / SlotSize
	}
	if aliasMap != nil && !rawPath {
		if alias := resolveAliasPath(aliasMap, st); alias != "" {
			result.DevicePath = alias
		}
	}
	return result
}

func printFindResult(result findResult, opts findOptions) {
	if opts.jsonOutput {
		blob, err := json.Marshal(result)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_JSON: %v\n", err)
			return
		}
		fmt.Println(string(blob))
		return
	}
	if opts.quiet {
		if result.Status == "match" {
			fmt.Println(result.DevicePath)
		}
		return
	}
	if result.Status == "match" {
		fmt.Printf("%s  size=%sGiB  ver=%d  cap_gb=%d  id=%s  note=%dB  grid_start=0x%X  index_len=%d  slots=%d\n",
			result.DevicePath,
			result.SizeGiB,
			result.Version,
			result.CapacityGB,
			result.DiskID,
			result.NoteBytes,
			result.GridStart,
			result.IndexLen,
			result.Slots,
		)
		return
	}
	if result.Status == "error" && opts.showErrors {
		fmt.Printf("%s  error=%s\n", result.DevicePath, result.Error)
	}
}

func writePanJSON(results []findResult) error {
	payload := panFile{
		Version: 1,
		Devices: results,
	}
	blob, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return os.WriteFile("pan.json", blob, 0644)
}

func buildDeviceAliasMap(root string) map[uint64]string {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	aliases := make(map[uint64]string)
	for _, entry := range entries {
		name := entry.Name()
		if strings.Contains(name, "-part") {
			continue
		}
		aliasPath := filepath.Join(root, name)
		info, err := os.Stat(aliasPath)
		if err != nil {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Rdev == 0 {
			continue
		}
		if _, exists := aliases[stat.Rdev]; exists {
			continue
		}
		aliases[stat.Rdev] = aliasPath
	}
	return aliases
}

func resolveAliasPath(aliasMap map[uint64]string, info os.FileInfo) string {
	if aliasMap == nil {
		return ""
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return aliasMap[stat.Rdev]
}
