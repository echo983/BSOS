package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/echo983/BSOS/pkg/bsospb"
	"github.com/echo983/BSOS/pkg/cdc"
	"github.com/echo983/BSOS/pkg/manifest"
	"github.com/zeebo/xxh3"
)

type UploadResult struct {
	Status      string `json:"status"` // "success" or "deduplicated"
	FID         string `json:"fid"`
	Size        int64  `json:"size_bytes"`
	Instant     bool   `json:"instant_deduplicated"`
	ChunksCount int    `json:"cdc_chunks,omitempty"`
	Message     string `json:"message,omitempty"`
}

func runUpload(args []string) int {
	fs := flag.NewFlagSet("bsos upload", flag.ContinueOnError)
	ticketOrURL := fs.String("ticket", "", "1-hour ephemeral upload ticket or full upload URL (e.g. https://domain/api/bsos/upload?ticket=...)")
	fs.StringVar(ticketOrURL, "u", "", "alias for --ticket")
	gatewayURL := fs.String("gateway", "", "Gateway base URL (defaults to ticket URL domain or https://edwins-arsenal-mcp.cambian.art)")
	asJSON := fs.Bool("json", false, "output JSON metadata")
	atomicMode := fs.Bool("atomic", false, "force atomic single-object upload (payload must be <= 16MB)")
	cdcMode := fs.Bool("cdc", false, "force FastCDC content-defined chunking")
	timeout := fs.Duration("timeout", 10*time.Minute, "timeout for entire upload")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos upload --ticket <TICKET_OR_URL> [options] [file|-]")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Client-side authenticated upload to BSOS Storage Gateway using a 1-hour ticket.")
		fmt.Println("  Computes XXH3_64 hash and FastCDC chunking strictly inside the sandbox.")
		fmt.Println("  Performs pre-check against gateway for Instant Zero-Bandwidth Deduplication (秒传).")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  bsos upload --ticket <TICKET> my_artifact.tar.gz")
		fmt.Println("  bsos upload -u \"https://edwins-arsenal-mcp.cambian.art/api/bsos/upload?ticket=...\" data.bin")
		fmt.Println("  cat output.log | bsos upload --ticket <TICKET> -")
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	rawTicket := strings.TrimSpace(*ticketOrURL)
	if rawTicket == "" {
		// Also check env var BSOS_UPLOAD_TICKET
		rawTicket = os.Getenv("BSOS_UPLOAD_TICKET")
	}
	if rawTicket == "" {
		fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: missing required upload ticket (--ticket <TICKET_OR_URL> or $BSOS_UPLOAD_TICKET)\n")
		fs.Usage()
		return 2
	}

	// Parse gateway and ticket token
	ticket := rawTicket
	targetGateway := *gatewayURL

	if strings.HasPrefix(rawTicket, "http://") || strings.HasPrefix(rawTicket, "https://") {
		parsed, err := url.Parse(rawTicket)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid upload URL: %v\n", err)
			return 2
		}
		targetGateway = fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
		ticketParam := parsed.Query().Get("ticket")
		if ticketParam != "" {
			ticket = ticketParam
		}
	}

	if targetGateway == "" {
		if envGw := os.Getenv("BSOS_GATEWAY_URL"); envGw != "" {
			targetGateway = strings.TrimRight(envGw, "/")
		} else {
			targetGateway = "https://edwins-arsenal-mcp.cambian.art"
		}
	}
	targetGateway = strings.TrimRight(targetGateway, "/")

	// Read input data
	var inputData []byte
	var err error

	if fs.NArg() == 0 || fs.Arg(0) == "-" {
		inputData, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_IO: failed to read stdin: %v\n", err)
			return 1
		}
	} else {
		filePath := fs.Arg(0)
		inputData, err = os.ReadFile(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_IO: failed to read file %s: %v\n", filePath, err)
			return 1
		}
	}

	totalSize := int64(len(inputData))
	httpClient := &http.Client{Timeout: *timeout}

	// Helper for checking if FID exists on gateway
	checkExists := func(fid uint64) (bool, error) {
		fidHex := fmt.Sprintf("0x%016x", fid)
		reqUrl := fmt.Sprintf("%s/api/bsos/check/%s?ticket=%s", targetGateway, fidHex, url.QueryEscape(ticket))
		req, err := http.NewRequestWithContext(context.Background(), "HEAD", reqUrl, nil)
		if err != nil {
			return false, err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return true, nil
		}
		if resp.StatusCode == http.StatusNotFound {
			return false, nil
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return false, fmt.Errorf("ticket rejected or expired (status %d)", resp.StatusCode)
		}
		return false, nil
	}

	// Helper for uploading a single payload (atomic or chunk)
	uploadPayload := func(fid uint64, payload []byte, isManifest bool) error {
		fidHex := fmt.Sprintf("0x%016x", fid)
		reqUrl := fmt.Sprintf("%s/api/bsos/upload?ticket=%s&fid=%s&size=%d", targetGateway, url.QueryEscape(ticket), fidHex, len(payload))
		if isManifest {
			reqUrl += "&manifest=true"
		}
		req, err := http.NewRequestWithContext(context.Background(), "POST", reqUrl, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-BSOS-FID", fidHex)

		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("upload failed (HTTP %d): %s", resp.StatusCode, string(body))
		}
		return nil
	}

	// Route Track 1: Atomic (<= 16MB) or forced atomic
	if (!*cdcMode && totalSize <= 16<<20) || *atomicMode {
		fid := xxh3.Hash(inputData)
		fidHex := fmt.Sprintf("0x%016x", fid)

		// 1. Pre-check for Instant Deduplication
		exists, checkErr := checkExists(fid)
		if checkErr != nil {
			fmt.Fprintf(os.Stderr, "E_PRECHECK: %v\n", checkErr)
			return 1
		}
		if exists {
			res := UploadResult{
				Status:  "deduplicated",
				FID:     fidHex,
				Size:    totalSize,
				Instant: true,
				Message: "Instant upload (content already exists on BSOS)",
			}
			if *asJSON {
				json.NewEncoder(os.Stdout).Encode(res)
			} else {
				fmt.Printf("Instant Upload (Deduplicated): %s (size: %d bytes)\n", fidHex, totalSize)
			}
			return 0
		}

		// 2. Upload atomic object
		if err := uploadPayload(fid, inputData, false); err != nil {
			fmt.Fprintf(os.Stderr, "E_UPLOAD: %v\n", err)
			return 1
		}

		res := UploadResult{
			Status:  "success",
			FID:     fidHex,
			Size:    totalSize,
			Instant: false,
		}
		if *asJSON {
			json.NewEncoder(os.Stdout).Encode(res)
		} else {
			fmt.Printf("Uploaded: %s (size: %d bytes)\n", fidHex, totalSize)
		}
		return 0
	}

	// Route Track 2: FastCDC chunking (> 16MB or --cdc)
	chunker, err := cdc.NewChunker(bytes.NewReader(inputData), cdc.Options{
		MinSize:    cdc.DefaultMinSize,
		TargetSize: cdc.DefaultTargetSize,
		MaxSize:    cdc.DefaultMaxSize,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_CDC: %v\n", err)
		return 1
	}

	var chunkDescriptors []*bsospb.ChunkDescriptor
	var offset uint64 = 0

	for {
		chunk, err := chunker.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "E_CDC_CHUNK: %v\n", err)
			return 1
		}

		chunkSize := chunk.Size
		chunkFid := chunk.FID

		// Check if chunk exists
		exists, err := checkExists(chunkFid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_PRECHECK: %v\n", err)
			return 1
		}

		if !exists {
			if err := uploadPayload(chunkFid, chunk.Data, false); err != nil {
				fmt.Fprintf(os.Stderr, "E_UPLOAD_CHUNK: %v\n", err)
				return 1
			}
		}

		chunkDescriptors = append(chunkDescriptors, &bsospb.ChunkDescriptor{
			Fid:         chunkFid,
			TargetFid:   chunkFid,
			Offset:      offset,
			Size:        chunkSize,
			JumpsTaken:  0,
		})
		offset += chunkSize
	}

	// Build and upload manifest
	fullContentHash := xxh3.Hash(inputData)
	fileManifest := &bsospb.FileManifest{
		Version:          1,
		TotalSize:        uint64(totalSize),
		FullContentHash:  fullContentHash,
		ChunkTargetSize:  uint64(cdc.DefaultTargetSize),
		Chunks:           chunkDescriptors,
	}

	manifestBytes, err := manifest.Encode(fileManifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_MANIFEST_MARSHAL: %v\n", err)
		return 1
	}

	rootFid := xxh3.Hash(manifestBytes)
	rootFidHex := fmt.Sprintf("0x%016x", rootFid)

	if err := uploadPayload(rootFid, manifestBytes, true); err != nil {
		fmt.Fprintf(os.Stderr, "E_UPLOAD_MANIFEST: %v\n", err)
		return 1
	}

	res := UploadResult{
		Status:      "success",
		FID:         rootFidHex,
		Size:        totalSize,
		Instant:     false,
		ChunksCount: len(chunkDescriptors),
	}

	if *asJSON {
		json.NewEncoder(os.Stdout).Encode(res)
	} else {
		fmt.Printf("CDC Uploaded Root Manifest: %s (total: %d bytes, chunks: %d)\n", rootFidHex, totalSize, len(chunkDescriptors))
	}
	return 0
}
