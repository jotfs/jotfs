package sum

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"

	"golang.org/x/crypto/blake2b"
)

// Size is the byte-size of a checksum
const Size = 32

// Sum stores a checksum
type Sum [Size]byte

// FromBytes converts a byte slice to a Sum. Its length must be sum.Size bytes.
func FromBytes(b []byte) (Sum, error) {
	if len(b) != Size {
		return Sum{}, fmt.Errorf("length must be %d not %d", Size, len(b))
	}
	var s Sum
	copy(s[:], b)
	return s, nil
}

// FromBase64 converts a base64 encoded string to a Sum.
func FromBase64(s string) (Sum, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return Sum{}, err
	}
	return FromBytes(b)
}

// Compute returns the checksum of a byte slice.
func Compute(data []byte) Sum {
	return blake2b.Sum256(data)
}

// AsHex returns the hex-encoded representation of s.
func (s Sum) AsHex() string {
	return hex.EncodeToString(s[:])
}

// Hash computes a checksum. Implements the `io.Writer` interface.
type Hash struct {
	h hash.Hash
}

// New returns a new Hash.
func New() (*Hash, error) {
	h, err := blake2b.New256(nil)
	if err != nil {
		return nil, err
	}
	return &Hash{h}, nil
}

// Write writes a byte slice to the hash function.
func (h *Hash) Write(p []byte) (int, error) {
	return h.h.Write(p)
}

// Sum returns the current checksum of a Hash.
func (h *Hash) Sum() Sum {
	b := h.h.Sum(nil)
	var s Sum
	copy(s[:], b)
	return s
}
