package compress

import (
	"fmt"
	"io"

	"github.com/valyala/gozstd"
)

// Mode is the compression mode
type Mode uint8

// Data compression modes
const (
	None Mode = 1
	Zstd Mode = 2
)

// AsUint8 converts a compression mode to a uint8.
func (m Mode) AsUint8() uint8 {
	return uint8(m)
}

// FromUint8 converts a uint8 to a compression mode. Returns an error if the value
// is an unknown mode.
func FromUint8(v uint8) (Mode, error) {
	if v >= 1 && v <= 2 {
		return Mode(v), nil
	}
	return 0, fmt.Errorf("invalid compression mode %d", v)
}

// Compress compresses src, appends it to dst, and returns the updated dst slice.
func (m Mode) Compress(dst []byte, src []byte) []byte {
	switch m {
	case None:
		dst = append(dst, src...)
		return dst
	case Zstd:
		dst = gozstd.Compress(dst, src)
		return dst
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
		return gozstd.StreamDecompress(dst, src)
	default:
		panic("not implemented")
	}

}
