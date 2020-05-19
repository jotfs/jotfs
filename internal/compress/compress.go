package compress

import (
	"fmt"
	"io"

	"github.com/DataDog/zstd"
)

// Mode is the compression mode
type Mode uint8

// Data compression modes
const (
	None Mode = 0
	Zstd Mode = 1
)

// AsUint8 converts a compression mode to a uint8.
func (m Mode) AsUint8() uint8 {
	return uint8(m)
}

// FromUint8 converts a uint8 to a compression mode. Returns an error if the value
// is an unknown mode.
func FromUint8(v uint8) (Mode, error) {
	if v <= 1 {
		return Mode(v), nil
	}
	return 0, fmt.Errorf("invalid compression mode %d", v)
}

// Compress compresses src, appends it to dst, and returns the updated dst slice.
func (m Mode) Compress(src []byte) ([]byte, error) {
	switch m {
	case None:
		dst := make([]byte, len(src))
		copy(dst, src)
		return dst, nil
	case Zstd:
		return zstd.Compress(nil, src)
	default:
		panic("not implemented")
	}
}

// DecompressStream decompresses data from src and writes it to dst.
func (m Mode) DecompressStream(dst io.Writer, src io.Reader) error {
	switch m {
	case None:
		_, err := io.Copy(dst, src)
		return err
	case Zstd:
		r := zstd.NewReader(src)
		_, err := io.Copy(dst, r)
		cerr := r.Close()
		if cerr != nil || err != nil {
			if cerr != nil {
				return fmt.Errorf("%v; %v", err, cerr)
			}
			return err
		}
		return nil
	default:
		panic("not implemented")
	}

}
