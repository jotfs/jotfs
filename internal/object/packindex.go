package object

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/sum"
)

// maxBlocks is an upper limit on the number of blocks allowed in a packfile.
const maxBlocks = 10_000

// BlockInfo stores information for a block within a packfile.
type BlockInfo struct {
	// Sum is the checksum of the chunk contained in the block.
	Sum sum.Sum
	// ChunkSize is the byte-size of the decompressed chunk
	ChunkSize uint64
	// Sequence is the sequence number of the block within the packfile.
	Sequence uint64
	// Offset is the byte-offset to the start of the block.
	Offset uint64
	// Size is the byte-size of the block.
	Size uint64
	// Compression is the algorithm used to compress the chunk.
	Mode compress.Mode
}

// PackIndex stores information about each chunk in a packfile.
type PackIndex struct {
	// Blocks is a slice of all blocks in the index.
	Blocks []BlockInfo
	// Sum is the checksum of the packfile corresponding to the index.
	Sum sum.Sum
	// Size is the size of the packfile underlying the index. This will not equal the
	// sum of the sizes of all blocks in the index because the file contains extra
	// metadata.
	Size uint64
}

// MarshalBinary converts a PackIndex to its binary representation.
func (p *PackIndex) MarshalBinary() []byte {
	// TODO: we can pre-calculate the buffer capacity here
	b := make([]byte, 0)
	b = append(b, p.Sum[:]...)
	b = append(b, uint64Binary(uint64(len(p.Blocks)))...)
	for _, bck := range p.Blocks {
		b = bck.marshalBinary(b)
	}
	return b
}

// UnmarshalBinary reads a pack index from its binary format.
func (p *PackIndex) UnmarshalBinary(data []byte) error {
	b := bytes.NewBuffer(data)

	var psum sum.Sum
	if _, err := b.Read(psum[:]); err != nil {
		return err
	}

	nBlocks, err := getBinaryUint64(b)
	if err != nil {
		return err
	}
	// Limit nBlocks to prevent an out-of-memory error. This check should fail only
	// if the pack index file is corrupted.
	if nBlocks > maxBlocks {
		return fmt.Errorf("number of blocks %d exceeds limit %d", nBlocks, maxBlocks)
	}

	blocks := make([]BlockInfo, nBlocks)
	for i := uint64(0); i < nBlocks; i++ {
		block := &blocks[i]
		block.unmarshalBinary(b)
	}

	p.Sum = psum
	p.Blocks = blocks

	return nil
}

func (b *BlockInfo) marshalBinary(buf []byte) []byte {
	buf = append(buf, b.Sum[:]...)
	buf = append(buf, uint64Binary(b.ChunkSize)...)
	buf = append(buf, uint64Binary(b.Sequence)...)
	buf = append(buf, uint64Binary(b.Offset)...)
	buf = append(buf, uint64Binary(b.Size)...)
	buf = append(buf, b.Mode.AsUint8())
	return buf
}

func (b *BlockInfo) unmarshalBinary(r io.Reader) error {
	var sum sum.Sum
	if _, err := r.Read(sum[:]); err != nil {
		return err
	}
	csize, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	seq, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	offset, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	size, err := getBinaryUint64(r)
	if err != nil {
		return err
	}
	var m [1]byte
	if _, err := r.Read(m[:]); err != nil {
		return err
	}
	mode, err := compress.FromUint8(m[0])
	if err != nil {
		return err
	}

	b.Sum = sum
	b.ChunkSize = csize
	b.Sequence = seq
	b.Offset = offset
	b.Size = size
	b.Mode = mode

	return nil
}

func uint64Binary(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func getBinaryUint64(r io.Reader) (uint64, error) {
	var n uint64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return 0, err
	}
	return n, nil
}
