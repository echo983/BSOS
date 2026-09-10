package main

import (
	"fmt"
	"os"

	"bsos/internal/blk"
	"bsos/internal/zram"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "blk":
		os.Exit(runBlk(os.Args[2:]))
	case "zram":
		os.Exit(runZram(os.Args[2:]))
	case "-h", "--help", "help":
		printUsage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "E_BAD_COMMAND: unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func runZram(args []string) int {
	if len(args) < 1 {
		printZramUsage()
		return 2
	}

	switch args[0] {
	case "create":
		return zram.RunCreate(args[1:])
	case "flush":
		return zram.RunFlush(args[1:])
	case "load":
		return zram.RunLoad(args[1:])
	case "-h", "--help", "help":
		printZramUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "E_BAD_COMMAND: unknown zram command: %s\n", args[0])
		printZramUsage()
		return 2
	}
}

func printZramUsage() {
	fmt.Println("Usage:")
	fmt.Println("  bsos zram create <size> [flags]")
	fmt.Println("  bsos zram flush [flags]")
	fmt.Println("  bsos zram load [flags]")
	fmt.Println("Notes:")
	fmt.Println("  zram requires root for device access")
	fmt.Println("  bsosd itself auto-loads snapshots at startup when -zram-snapshot-dir")
	fmt.Println("  is set (docs/DESIGN.md §3.13) - manual `zram load` is for standalone use")
}

func runBlk(args []string) int {
	if len(args) < 1 {
		printBlkUsage()
		return 2
	}

	switch args[0] {
	case "init":
		return blk.RunInit(args[1:])
	case "find":
		return blk.RunFind(args[1:])
	case "info":
		return blk.RunInfo(args[1:])
	case "index-backup":
		return blk.RunIndexBackup(args[1:])
	case "index-restore":
		return blk.RunIndexRestore(args[1:])
	case "index-compact":
		return blk.RunIndexCompact(args[1:])
	case "packed-scrub":
		return blk.RunPackedScrub(args[1:])
	case "-h", "--help", "help":
		printBlkUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "E_BAD_COMMAND: unknown blk command: %s\n", args[0])
		printBlkUsage()
		return 2
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  bsos blk init <device> [flags]")
	fmt.Println("  bsos blk find [flags]")
	fmt.Println("  bsos blk info <device>")
	fmt.Println("  bsos blk index-backup [flags]")
	fmt.Println("  bsos blk index-restore <device> [flags]")
	fmt.Println("  bsos blk index-compact [flags]")
	fmt.Println("  bsos blk packed-scrub [flags]")
	fmt.Println("  bsos zram create <size> [flags]")
	fmt.Println("  bsos zram flush [flags]")
	fmt.Println("  bsos zram load [flags]")
	fmt.Println("Notes:")
	fmt.Println("  init/find require root when accessing block devices")
	fmt.Println("  find writes pan.json in the current directory")
}

func printBlkUsage() {
	fmt.Println("Usage:")
	fmt.Println("  bsos blk init <device> [flags]")
	fmt.Println("  bsos blk find [flags]")
	fmt.Println("  bsos blk info <device>")
	fmt.Println("  bsos blk index-backup [flags]")
	fmt.Println("  bsos blk index-restore <device> [flags]")
	fmt.Println("  bsos blk index-compact [flags]")
	fmt.Println("  bsos blk packed-scrub [flags]")
	fmt.Println("Flags:")
	fmt.Println("  --note \"...\"    Note for Master Header (<= 4080 bytes)")
	fmt.Println("  --id <uint64|auto> Disk ID (default: auto)")
	fmt.Println("  --force           Allow overwriting existing header/index")
	fmt.Println("  --yes             Skip confirmation")
	fmt.Println("  --dry-run         Print layout only, no writes")
	fmt.Println("  --all             Scan all /sys/block entries")
	fmt.Println("  --include loop,dm Include extra device classes")
	fmt.Println("  --path /dev/sdX   Scan specific device path (repeatable)")
	fmt.Println("  --json            JSONL output")
	fmt.Println("  --quiet           Only print matched device paths")
	fmt.Println("  --show-errors     Include unreadable devices with error reasons")
	fmt.Println("  --raw-path        Keep original /dev path (do not replace with /dev/disk/by-id)")
	fmt.Println("  --pan <path>      pan.json path (default: pan.json)")
	fmt.Println("  --disk <id>       Target disk id (default: all in pan.json)")
	fmt.Println("  --file <path>     Backup file for index-restore/index-compact")
	fmt.Println("  --fix             Remove invalid packed-anchor entries and rewrite index")
}
