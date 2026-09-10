package pan

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type File struct {
	Version int      `json:"version"`
	Devices []Device `json:"devices"`
}

type Device struct {
	Status     string `json:"status"`
	DevicePath string `json:"device_path"`
	DiskID     string `json:"disk_id"`
	Version    uint16 `json:"version"`
	SizeBytes  uint64 `json:"size_bytes"`
}

func Read(path string) (File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	var pan File
	if err := json.Unmarshal(raw, &pan); err != nil {
		return File{}, err
	}
	return pan, nil
}

func SelectDevice(pan File, panID string) (Device, error) {
	if len(pan.Devices) == 0 {
		return Device{}, fmt.Errorf("no devices in pan.json")
	}
	if panID == "" {
		return pan.Devices[0], nil
	}
	want, err := ParseDiskID(panID)
	if err != nil {
		return Device{}, err
	}
	needle := fmt.Sprintf("0x%X", want)
	for _, dev := range pan.Devices {
		if strings.EqualFold(dev.DiskID, needle) {
			return dev, nil
		}
	}
	return Device{}, fmt.Errorf("disk id %s not found in pan.json", needle)
}

func ParseDiskID(input string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(input), 0, 64)
}

func NormalizeID(input string) string {
	if strings.HasPrefix(input, "0x") || strings.HasPrefix(input, "0X") {
		return "0x" + strings.ToUpper(strings.TrimPrefix(strings.TrimPrefix(input, "0x"), "0X"))
	}
	if parsed, err := strconv.ParseUint(input, 10, 64); err == nil {
		return fmt.Sprintf("0x%X", parsed)
	}
	return input
}
