package object

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/sum"
)

// PackfileBuilder is used to build a packfile object.
type PackfileBuilder struct {
	w      *countingWriter
	idx    []BlockInfo
	seen   map[sum.Sum]bool
	seq    uint64
	closed bool
	hash   *sum.Hash
}

// NewPackfileBuilder creates a new PackfileBuilder
func NewPackfileBuilder(w io.Writer) (*PackfileBuilder, error) {
	hash, err := sum.New()
	if err != nil {
		return nil, err
	}
	// Send everything writen to the packfile through the hash function
	w = io.MultiWriter(w, hash)
	cw := countingWriter{w, 0}

	// Write the object type
	if _, err := cw.Write([]byte{PackfileObject}); err != nil {
		return nil, err
	}

	b := PackfileBuilder{
		&cw,
		make([]BlockInfo, 0),
		make(map[sum.Sum]bool),
		0,
		false,
		hash,
	}
	return &b, nil
}

// Append writes a chunk of data to packfile owned by the builder, and returns the
// chunk checksum.
func (b *PackfileBuilder) Append(data []byte, mode compress.Mode) (sum.Sum, error) {
	if b.closed {
		return sum.Sum{}, errors.New("packfile builder is closed")
	}
	s := sum.Compute(data)
	if _, ok := b.seen[s]; ok {
		// The packfile already contains this chunk
		return s, nil
	}

	// Write the block, with compressed chunk data, to the packfile
	offset := b.w.bytesWritten // Need to get offset before write
	block, err := makeBlock(data, s, mode)
	if err != nil {
		return sum.Sum{}, err
	}
	if _, err := b.w.Write(block); err != nil {
		return sum.Sum{}, err
	}

	info := BlockInfo{
		Sum:       s,
		ChunkSize: uint64(len(data)),
		Sequence:  b.seq,
		Offset:    offset,
		Size:      uint64(len(block)),
		Mode:      mode,
	}
	b.idx = append(b.idx, info)

	// Update the builder for the next chunk
	b.seq++
	b.seen[s] = true

	return s, nil
}

// Build returns the pack index for the packfile owned by the builder. Subsequent
// calls to append a chunk will return an error.
func (b *PackfileBuilder) Build() PackIndex {
	b.closed = true
	return PackIndex{Blocks: b.idx, Sum: b.hash.Sum()}
}

type block struct {
	Sum    sum.Sum
	Mode   compress.Mode
	Data   []byte
	Offset uint64
	Size   uint64
}

// LoadPackIndex reads a packfile and generates its pack index.
func LoadPackIndex(r io.Reader) (PackIndex, error) {
	// Send all data read from the packfile through a hash function
	phash, err := sum.New()
	if err != nil {
		return PackIndex{}, err
	}
	tee := io.TeeReader(r, phash)
	cr := &countingReader{tee, 0}

	// Read object type
	var objType uint8
	if err := binary.Read(cr, binary.LittleEndian, &objType); err != nil {
		return PackIndex{}, fmt.Errorf("reading object type: %w", err)
	}
	if objType != PackfileObject {
		return PackIndex{}, fmt.Errorf("expected packfile object but received object type %d", objType)
	}

	// Read each block from the packfile and parse its BlockInfo
	idx := make([]BlockInfo, 0)
	for seq := uint64(0); ; seq++ {
		block, err := readBlock(cr)
		if err == io.EOF {
			break
		} else if err != nil {
			return PackIndex{}, fmt.Errorf("reading block %d: %w", seq, err)
		}

		// Decompress the data and verify the chunk's checksum
		rd := bytes.NewBuffer(block.Data)
		chash, err := sum.New()
		cw := &countingWriter{chash, 0}
		if err != nil {
			return PackIndex{}, err
		}
		if err := block.Mode.DecompressStream(cw, rd); err != nil {
			return PackIndex{}, fmt.Errorf("decompressing chunk data in block %d: %w", seq, err)
		}
		actual := chash.Sum()
		if actual != block.Sum {
			return PackIndex{}, fmt.Errorf(
				"invalid chunk data in block %d. Expected checksum %x but actual checksum is %x",
				seq, block.Sum, actual,
			)
		}

		info := BlockInfo{
			Sum:       block.Sum,
			ChunkSize: cw.bytesWritten,
			Sequence:  seq,
			Offset:    block.Offset,
			Size:      block.Size,
			Mode:      block.Mode,
		}
		idx = append(idx, info)
	}

	return PackIndex{Blocks: idx, Sum: phash.Sum()}, nil
}

func makeBlock(data []byte, sum sum.Sum, mode compress.Mode) ([]byte, error) {
	// Reserve the first 8 bytes for the size of the compressed data
	block := make([]byte, 8)
	block = append(block, mode.AsUint8())
	block = append(block, sum[:]...)
	block = mode.Compress(block, data)

	// TODO: if the compressed size is larger we should store the orignal instead
	compressedSize := len(block) - (8 + 1 + len(sum))
	binary.LittleEndian.PutUint64(block[:8], uint64(compressedSize))

	return block, nil
}

func readBlock(r *countingReader) (block, error) {
	offset := r.bytesRead

	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return block{}, err
	}
	var m uint8
	if err := binary.Read(r, binary.LittleEndian, &m); err != nil {
		return block{}, err
	}
	mode, err := compress.FromUint8(m)
	if err != nil {
		return block{}, fmt.Errorf("invalid compression mode %d", mode)
	}
	var s sum.Sum
	if _, err := r.Read(s[:]); err != nil {
		return block{}, err
	}
	// TODO: put upper limit on size to prevent out-of-memory error
	compressed := make([]byte, size)
	if _, err := r.Read(compressed); err != nil {
		return block{}, err
	}

	blockSize := r.bytesRead - offset
	return block{Sum: s, Mode: mode, Data: compressed, Size: blockSize, Offset: offset}, nil
}

type countingReader struct {
	reader    io.Reader
	bytesRead uint64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.bytesRead += uint64(n)
	return n, err
}

type countingWriter struct {
	writer       io.Writer
	bytesWritten uint64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.bytesWritten += uint64(n)
	return n, err
}
