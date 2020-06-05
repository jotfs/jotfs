package object

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/jotfs/jotfs/internal/compress"
	"github.com/jotfs/jotfs/internal/sum"
)

// PackfileBuilder is used to build a packfile object.
type PackfileBuilder struct {
	w      *countingWriter
	idx    []BlockInfo
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

	b := PackfileBuilder{
		w:      &cw,
		idx:    make([]BlockInfo, 0),
		seq:    0,
		closed: false,
		hash:   hash,
	}
	return &b, nil
}

// Append writes a chunk of data to packfile owned by the builder.
func (b *PackfileBuilder) Append(data []byte, s sum.Sum, mode compress.Mode) error {
	if len(b.idx) == 0 {
		// Write the object type
		if _, err := b.w.Write([]byte{PackfileObject}); err != nil {
			return err
		}

	}
	if b.closed {
		return errors.New("packfile builder is closed")
	}

	// Write the block, with compressed chunk data, to the packfile
	// TODO: a partial write here will lead to a corrupted packfile. Ideally, the write
	// should complete in entirety, or fail with 0 bytes written.
	offset := b.w.bytesWritten // Need to get offset before write
	block, err := makeBlock(data, s, mode)
	if err != nil {
		return err
	}
	if _, err := b.w.Write(block); err != nil {
		return err
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

	return nil
}

// Build returns the pack index for the packfile owned by the builder. Subsequent
// calls to append a chunk will return an error.
func (b *PackfileBuilder) Build() PackIndex {
	b.closed = true
	return PackIndex{Blocks: b.idx, Sum: b.hash.Sum(), Size: b.w.bytesWritten}
}

// BytesWritten returns the number of bytes written to the packfile.
func (b *PackfileBuilder) BytesWritten() uint64 {
	return b.w.bytesWritten
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

	return PackIndex{Blocks: idx, Sum: phash.Sum(), Size: cr.bytesRead}, nil
}

func makeBlock(data []byte, s sum.Sum, mode compress.Mode) ([]byte, error) {
	compressed, err := mode.Compress(data)
	if err != nil {
		return nil, err
	}

	capacity := 8 + 1 + sum.Size + len(data)
	block := make([]byte, 8, capacity)

	binary.LittleEndian.PutUint64(block[:8], uint64(len(compressed)))
	block = append(block, mode.AsUint8())
	block = append(block, s[:]...)
	block = append(block, compressed...)

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
	if _, err := io.ReadFull(r, s[:]); err != nil {
		return block{}, err
	}
	// TODO: put upper limit on size to prevent out-of-memory error
	compressed := make([]byte, size)
	if _, err := io.ReadFull(r, compressed); err != nil {
		return block{}, err
	}

	blockSize := r.bytesRead - offset
	return block{Sum: s, Mode: mode, Data: compressed, Size: blockSize, Offset: offset}, nil
}

func copyBlock(r io.Reader, w io.Writer) error {
	// Get the size of the compressed data
	var size uint64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return err
	}
	// Write the size and remaining data in the block to w
	if _, err := w.Write(uint64Binary(size)); err != nil {
		return err
	}
	if _, err := io.CopyN(w, r, 1+sum.Size+int64(size)); err != nil {
		return err
	}
	return nil
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

// FilterPackfile reads a packfile from r and writes each block satisfying the filter
// function to a new packfile w. The filter function f takes the sequence number of a
// block in the original packfile and should return true if that block should be kept.
func FilterPackfile(r io.Reader, w io.Writer, f func(uint64) bool) (uint64, error) {
	// Validate object type
	var objType uint8
	if err := binary.Read(r, binary.LittleEndian, &objType); err != nil {
		return 0, fmt.Errorf("reading object type: %w", err)
	}
	if objType != PackfileObject {
		return 0, fmt.Errorf("expected packfile object but received object type %d", objType)
	}

	cw := countingWriter{w, 0}

	// Copy all blocks satisfying the filter from r to cw
	var nBlocks int
	for i := uint64(0); ; i++ {
		var err error
		if f(i) {
			if nBlocks == 0 {
				// Write the file header
				if _, err := cw.Write([]byte{objType}); err != nil {
					return cw.bytesWritten, err
				}
			}
			nBlocks++
			err = copyBlock(r, &cw)
		} else {
			err = copyBlock(r, ioutil.Discard)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return cw.bytesWritten, fmt.Errorf("copying block %d: %w", i, err)
		}
	}

	return cw.bytesWritten, nil
}
