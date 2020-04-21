package upload

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/log"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/service"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/iotafs/iotafs/internal/sum"

	"github.com/google/uuid"
)

const (
	miB                  = 1024 * 1024
	packSizeLimit uint64 = 128 * miB
)

// Config stores the configuration for an uplaoad service.
type Config struct {
	Mode   compress.Mode
	Bucket string
}

type Service struct {
	db      *db.Adapter
	store   store.Store
	cfg     Config
	uploads map[string]*fileUpload
}

// fileUpload keeps track of which packfiles have been received for an in-progress
// file creation.
type fileUpload struct {
	sync.RWMutex
	file       object.File
	blueprints []PackBlueprint
	received   map[uint]bool
	processing map[uint]bool
}

func newFileUpload(file object.File, bps []PackBlueprint) *fileUpload {
	return &fileUpload{sync.RWMutex{}, file, bps, make(map[uint]bool), make(map[uint]bool)}
}

func (f *fileUpload) setProcessing(id uint) {
	f.Lock()
	defer f.Unlock()
	f.processing[id] = true
}

// isProcessing checks if a
func (f *fileUpload) isProcessing(id uint) bool {
	f.RLock()
	defer f.RUnlock()
	_, ok := f.processing[id]
	return ok
}

func (f *fileUpload) unsetProcessing(id uint) {
	f.Lock()
	defer f.Unlock()
	delete(f.processing, id)
}

func (f *fileUpload) setReceived(id uint) {
	f.Lock()
	defer f.Unlock()
	f.received[id] = true
}

func (f *fileUpload) getReceived() []uint {
	f.RLock()
	defer f.RUnlock()
	res := make([]uint, 0, len(f.received))
	for k := range f.received {
		res = append(res, k)
	}
	return res
}

func (f *fileUpload) allReceived() bool {
	f.RLock()
	defer f.RUnlock()
	return len(f.received) == len(f.blueprints)
}

func New(cfg Config, db *db.Adapter, store store.Store) *Service {
	return &Service{db, store, cfg, make(map[string]*fileUpload)}
}

func (s *Service) CreateFile(file object.File) (CreateFileResponse, error) {
	// TODO: check file is valid (no missing sequence numbers)

	// Ask the database which of the chunks in the file already exist
	sums := make([]sum.Sum, len(file.Chunks))
	for i, c := range file.Chunks {
		sums[i] = c.Sum
	}
	exists, err := s.db.ChunksExist(sums)
	if err != nil {
		return CreateFileResponse{}, err
	}

	// Add the chunks which don't already exist to a collection of packfile blueprints
	psums := make(map[sum.Sum]bool)
	blueprints := make([]PackBlueprint, 0)
	chunks := make([]object.Chunk, 0)
	size := uint64(0)
	for i, ok := range exists {
		c := file.Chunks[i]

		// A file may have chunks with the same checksum at different sequence numbers.
		// We only want to include the first one
		if _, sok := psums[c.Sum]; sok || ok {
			continue
		}

		// Start a new blueprint if the current one is at the size limit
		if size > packSizeLimit {
			cs := make([]object.Chunk, len(chunks))
			copy(cs, chunks)
			blueprints = append(blueprints, PackBlueprint{Size: size, Chunks: cs})
			size = uint64(0)
			chunks = chunks[:0]
		}

		psums[c.Sum] = true
		chunks = append(chunks, c)
		size += c.Size
	}
	if size > 0 {
		blueprints = append(blueprints, PackBlueprint{Size: size, Chunks: chunks})
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return CreateFileResponse{}, err
	}
	uploadID := id.String()

	s.uploads[uploadID] = newFileUpload(file, blueprints)

	return CreateFileResponse{uploadID, blueprints}, nil
}

func (s *Service) UploadPack(uploadID string, blueprintID uint, r io.Reader) error {
	upload, err := s.getUpload(uploadID)
	if err != nil {
		return nil
	}
	if blueprintID >= uint(len(upload.blueprints)) {
		return service.Errorf(service.NotFound, "blueprint %d for upload %s", blueprintID, uploadID)
	}
	if upload.isProcessing(blueprintID) {
		return service.Errorf(service.Conflict, "blueprint %d for upload %s is being processed", blueprintID, uploadID)
	}

	// Move the blueprint to the processing stage
	upload.setProcessing(blueprintID)
	defer upload.unsetProcessing(blueprintID)
	blueprint := upload.blueprints[blueprintID]

	// Create a file with a temporary name in the store for the packfile
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	tmpKey := id.String()
	pfile := s.store.NewFile(s.cfg.Bucket, tmpKey)

	// Write the contents of r to the packfile
	builder, err := object.NewPackfileBuilder(pfile)
	if err != nil {
		log.OnError(pfile.Cancel)
		return fmt.Errorf("creating packfile builder: %w", err)
	}
	if err := buildPackfileWithBlueprint(r, builder, blueprint, s.cfg.Mode); err != nil {
		log.OnError(pfile.Cancel)
		return fmt.Errorf("building packfile: %w", err)
	}
	index := builder.Build()
	if err := pfile.Close(); err != nil {
		return err
	}
	packID := index.Sum

	// Rename the temp file to the packfile checksum
	fkey := service.PackfileKey(packID)
	if err := s.moveFile(tmpKey, fkey); err != nil {
		s.deleteFileLogOnError(tmpKey)
		return fmt.Errorf("moving packfile from %s to %s: %w", tmpKey, fkey, err)
	}

	// Save the index to the store and the database
	ikey := service.PackIndexKey(packID)
	ifile := s.store.NewFile(s.cfg.Bucket, ikey)
	if _, err := ifile.Write(index.MarshalBinary()); err != nil {
		log.OnError(ifile.Cancel)
		s.deleteFileLogOnError(fkey)
		return fmt.Errorf("writing to store file %s", ikey)
	}
	if err := s.db.InsertPackIndex(index); err != nil {
		s.deleteFileLogOnError(fkey)
		s.deleteFileLogOnError(ikey)
		return fmt.Errorf("inserting pack index: %w", err)
	}

	upload.setReceived(blueprintID)

	if upload.allReceived() {
		// Save the File to the database
		s.db.InsertFileWithNewVersion(upload.file)
	}

	return nil
}

func buildPackfileWithBlueprint(r io.Reader, b *object.PackfileBuilder, bp PackBlueprint, mode compress.Mode) error {
	// Get the maximum chunk size for setting the buffer capacity
	var max uint64
	for _, c := range bp.Chunks {
		if c.Size > max {
			max = c.Size
		}
	}
	buf := make([]byte, max)

	// Read chunks from r according to the blueprint and append them to the pack builder
	for _, c := range bp.Chunks {
		bb := buf[:c.Size]

		if _, err := io.ReadFull(r, bb); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return service.Errorf(service.BadArgument, "EOF reading data for sequence %d", c.Sequence)
			}
			return err
		}

		if sum, err := b.Append(bb, mode); err != nil {
			return fmt.Errorf("appending to packfile builder: %w", err)
		} else if sum != c.Sum {
			return service.Errorf(service.BadArgument, "expected sum %x not %x for sequence %d", c.Sum, sum, c.Sequence)
		}
	}
	return nil
}

func (s *Service) getUpload(uploadID string) (*fileUpload, error) {
	if uploadID == "" {
		return nil, service.Error(service.BadArgument, "upload ID is empty")
	}
	upload, ok := s.uploads[uploadID]
	if !ok {
		return nil, service.Errorf(service.NotFound, "upload ID %s not found", uploadID)
	}
	return upload, nil
}

func (s *Service) deleteFileLogOnError(key string) {
	log.OnError(func() error { return s.store.Delete(s.cfg.Bucket, key) })
}

func (s *Service) moveFile(from string, to string) error {
	if err := s.store.Copy(s.cfg.Bucket, from, to); err != nil {
		return err
	}
	return s.store.Delete(s.cfg.Bucket, from)
}
