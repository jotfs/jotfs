package object

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/iotafs/iotafs/internal/sum"
)

const maxChunks = 1_000_000
const maxNameSize = 32768

// File represents a file object.
type File struct {
	Name      string
	CreatedAt time.Time
	Chunks    []Chunk
	Versioned bool
}

// Chunk stores the information for a chunk within a file object.
type Chunk struct {
	Sequence uint64
	Size     uint64
	Sum      sum.Sum
}

// MarshalBinary writes the binary representation of a file to a writer.
func (f *File) MarshalBinary() []byte {
	var vtag uint8
	if f.Versioned {
		vtag = 1
	}

	b := make([]byte, 0)
	b = append(b, uint64Binary(uint64(len(f.Name)))...)
	b = append(b, []byte(f.Name)...)
	b = append(b, uint64Binary(uint64(f.CreatedAt.UnixNano()))...)
	b = append(b, vtag)
	b = append(b, uint64Binary(uint64(len(f.Chunks)))...)
	buf := make([]byte, 0)
	for _, c := range f.Chunks {
		buf = c.marshalBinary(buf)
		b = append(b, buf...)
		buf = buf[:0]
	}
	return b
}

// UnmarshalBinary reads the binary representation of a file from a reader.
func (f *File) UnmarshalBinary(r io.Reader) error {
	nameSize, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	if nameSize > maxNameSize {
		return fmt.Errorf("file name length %d exceeds maximum %d", nameSize, maxNameSize)
	}

	name := make([]byte, nameSize)
	if _, err := r.Read(name); err != nil {
		return err
	}

	createdAtNanos, err := getBinaryUint64(r)
	if err != nil {
		return err
	}

	var vtag uint8
	if err := binary.Read(r, binary.LittleEndian, &vtag); err != nil {
		return fmt.Errorf("decoding versioning tag: %v", err)
	}
	var versioned bool
	if vtag == 1 {
		versioned = true
	} else if vtag != 0 {
		return fmt.Errorf("invalid versioning tag %d", vtag)
	}

	nChunks, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	if nChunks > maxChunks {
		return fmt.Errorf("number of chunks %d exceeds maximum %d", nChunks, maxChunks)
	}

	chunks := make([]Chunk, nChunks)
	for i := uint64(0); i < nChunks; i++ {
		c := &chunks[i]
		c.unmarshalBinary(r)
	}

	f.Name = string(name)
	f.CreatedAt = time.Unix(0, int64(createdAtNanos)).UTC()
	f.Chunks = chunks
	f.Versioned = versioned

	return nil
}

// Size returns the byte-size of the file.
func (f File) Size() uint64 {
	size := uint64(0)
	for _, c := range f.Chunks {
		size += c.Size
	}
	return size
}

func (c Chunk) marshalBinary(b []byte) []byte {
	b = append(b, uint64Binary(c.Sequence)...)
	b = append(b, uint64Binary(c.Size)...)
	b = append(b, c.Sum[:]...)
	return b
}

func (c *Chunk) unmarshalBinary(r io.Reader) error {
	seq, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	size, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	var sum sum.Sum
	if _, err := r.Read(sum[:]); err != nil {
		return err
	}

	c.Sequence = seq
	c.Size = size
	c.Sum = sum
	return nil
}
