package object

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/iotafs/iotafs/internal/sum"
)

const maxChunks = 1_000_000
const maxNameSize = 32768

// File represents a file object.
type File struct {
	Name    string
	Version uint
	Chunks  []Chunk
}

// Chunk stores the inforamtion for a chunk within a file object.
type Chunk struct {
	Sequence uint64
	Size     uint64
	Sum      sum.Sum
}

// MarshalBinary writes the binary representation of a file to a writer.
func (f *File) MarshalBinary(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(len(f.Name))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, f.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(f.Version)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(f.Chunks))); err != nil {
		return err
	}
	for _, c := range f.Chunks {
		if err := c.marshalBinary(w); err != nil {
			return err
		}
	}
	return nil
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

	version, err := getBinaryUint64(r)
	if err != nil {
		return err
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
	f.Version = uint(version)
	f.Chunks = chunks

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

func (c Chunk) marshalBinary(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, c.Sequence); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, c.Size); err != nil {
		return err
	}
	if _, err := w.Write(c.Sum[:]); err != nil {
		return err
	}
	return nil
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
