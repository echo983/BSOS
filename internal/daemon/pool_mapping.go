package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"bsos/internal/pan"
)

// Keep unavailable configured tiers so a later boot can recover them, while
// persisting resolved paths and newly discovered snapshots for CLI tooling.
func persistPoolMapping(path string, original pan.File, runtime []pan.Device) error {
	next := pan.File{Version: original.Version, Devices: append([]pan.Device(nil), original.Devices...)}
	if next.Version == 0 {
		next.Version = 1
	}
	for _, d := range runtime {
		id, err := pan.ParseDiskID(d.DiskID)
		if err != nil {
			return err
		}
		found := false
		for i, old := range next.Devices {
			oldID, err := pan.ParseDiskID(old.DiskID)
			if err != nil {
				return err
			}
			if oldID == id {
				next.Devices[i] = d
				found = true
				break
			}
		}
		if !found {
			next.Devices = append(next.Devices, d)
		}
	}
	old, _ := json.Marshal(original)
	raw, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	compact, _ := json.Marshal(next)
	if bytes.Equal(old, compact) {
		return nil
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".bsos-pan-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if st, err := os.Stat(path); err == nil {
		if err = f.Chmod(st.Mode().Perm()); err != nil {
			f.Close()
			return err
		}
	}
	if _, err = f.Write(append(raw, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return fmt.Errorf("sync pool mapping directory: %w", err)
	}
	return nil
}
