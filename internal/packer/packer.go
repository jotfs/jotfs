package packer

import (
	"errors"

	"github.com/google/uuid"
	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/log"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/iotafs/iotafs/internal/sum"
)

const (
	packFilePrefix  = "packfile/"
	packIndexPrefix = "packindex/"
)

// Packer takes a stream of chunk data and writes each to a packfile in the order they
// are recieved. The packer can be flushed to start a new packfile.
type Packer struct {
	cfg   Config
	store store.Store

	nchunks int
	builder *object.PackfileBuilder
	file    store.WriteCanceller
	id      uuid.UUID
}

// Config stores the configuration for a Packer.
type Config struct {
	// Mode is the compression mode used to compress each chunk in the packfile.
	Mode compress.Mode

	// Bucket is the bucket the packer will write files to.
	Bucket string
}

// New creates a new packer.
func New(s store.Store, cfg Config) *Packer {
	return &Packer{cfg: cfg, store: s}
}

// AddChunk adds a chunk of data, with a given sum, to the current packfile owned by
// the packer.
func (p *Packer) AddChunk(data []byte, sum sum.Sum) error {
	if p.builder == nil {
		if err := p.initBuilder(); err != nil {
			return err
		}
	}
	// TODO: pass sum along as parameter to Append so it doesn't need to calculate it
	_, err := p.builder.Append(data, p.cfg.Mode)
	if err != nil {
		return err
	}
	p.nchunks++

	return nil
}

// Flush closes the current packfile owned by the packer, committing the packfile and
// its associated pack index to the store. Returns the the packfile store id and the
// pack index. Returns an error if the current packfile has no chunks.
func (p *Packer) Flush() (string, object.PackIndex, error) {
	if p.nchunks == 0 {
		return "", object.PackIndex{}, errors.New("builder is empty")
	}
	if err := p.file.Close(); err != nil {
		return "", object.PackIndex{}, err
	}
	id := p.id.String()
	index := p.builder.Build()
	indexB := index.MarshalBinary()
	iKey := packIndexPrefix + id
	if err := putObject(p.store, p.cfg.Bucket, iKey, indexB); err != nil {
		return "", object.PackIndex{}, err
	}

	p.file = nil
	p.builder = nil
	p.nchunks = 0

	return id, index, nil
}

// Size returns the byte-size of the current packfile owned by the packer.
func (p *Packer) Size() uint64 {
	if p.builder == nil {
		return 0
	}
	return p.builder.BytesWritten()
}

// NumChunks returns the number of chunks in the current packfile owned by the packer.
// NumChunks can return 0 even if Size is greater than zero.
func (p *Packer) NumChunks() int {
	return p.nchunks
}

func (p *Packer) initBuilder() error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	key := packFilePrefix + id.String()
	f := p.store.NewFile(p.cfg.Bucket, key)
	b, err := object.NewPackfileBuilder(f)
	if err != nil {
		return err
	}
	p.builder = b
	p.file = f
	p.nchunks = 0
	p.id = id
	return nil
}

func putObject(s store.Store, bucket string, key string, data []byte) error {
	f := s.NewFile(bucket, key)
	_, err := f.Write(data)
	if err != nil {
		log.OnError(f.Cancel)
		return err
	}
	return f.Close()
}
