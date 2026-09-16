package manifest

import (
	"bytes"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/echo983/BSOS/internal/daemon/bsospb"
)

var (
	// MagicHeader is the 5-byte header prefixed to all serialized BSOS file manifests ("BSMN\x01").
	MagicHeader = []byte{'B', 'S', 'M', 'N', 0x01}

	// ErrInvalidMagic is returned when decoding data that does not start with MagicHeader.
	ErrInvalidMagic = errors.New("manifest: invalid magic header (not a BSOS FileManifest)")

	// ErrUnsupportedVersion is returned when decoding a manifest with an unknown version.
	ErrUnsupportedVersion = errors.New("manifest: unsupported manifest version")
)

const (
	// CurrentVersion is the current FileManifest specification version.
	CurrentVersion uint32 = 1
)

// IsManifest checks whether data begins with the BSOS Manifest MagicHeader.
func IsManifest(data []byte) bool {
	return len(data) >= len(MagicHeader) && bytes.Equal(data[:len(MagicHeader)], MagicHeader)
}

// Encode serializes a FileManifest into bytes prepended with MagicHeader.
func Encode(m *bsospb.FileManifest) ([]byte, error) {
	if m == nil {
		return nil, errors.New("manifest: cannot encode nil FileManifest")
	}
	if m.Version == 0 {
		m.Version = CurrentVersion
	}

	payload, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("manifest: proto marshal: %w", err)
	}

	buf := make([]byte, len(MagicHeader)+len(payload))
	copy(buf, MagicHeader)
	copy(buf[len(MagicHeader):], payload)

	return buf, nil
}

// Decode parses a serialized manifest byte slice (starting with MagicHeader) into a FileManifest.
func Decode(data []byte) (*bsospb.FileManifest, error) {
	if !IsManifest(data) {
		return nil, ErrInvalidMagic
	}

	payload := data[len(MagicHeader):]
	m := &bsospb.FileManifest{}
	if err := proto.Unmarshal(payload, m); err != nil {
		return nil, fmt.Errorf("manifest: proto unmarshal: %w", err)
	}

	if m.Version != CurrentVersion {
		return nil, fmt.Errorf("%w: got %d, expected %d", ErrUnsupportedVersion, m.Version, CurrentVersion)
	}

	return m, nil
}
