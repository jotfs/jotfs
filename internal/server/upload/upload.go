package upload

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/log"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/packer"
	pb "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/server"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/iotafs/iotafs/internal/sum"

	"github.com/google/uuid"
	"github.com/twitchtv/twirp"
)

const maxPackfileSize = 1024 * 1024 * 128 // TODO: pass as config parameter

// Server implements the FileUploader interface.
type Server struct {
	db      *db.Adapter
	store   store.Store
	uploads map[string]*fileUpload
	cfg     Config
}

// Config stores the configuration for the Server.
type Config struct {
	// Bucket is the bucket the server saves files to.
	Bucket string

	// QSize is the maximum number of chunks waiting to be uplaoded for any single
	// file upload.
	QSize uint

	// Mode is the default compression mode used to compress chunk data.
	Mode compress.Mode
}

// NewServer creates a new Server.
func NewServer(adapter *db.Adapter, store store.Store, cfg Config) *Server {
	return &Server{adapter, store, make(map[string]*fileUpload), cfg}
}

// Init initializes a new file upload.
func (s *Server) Init(ctx context.Context, _ *pb.Empty) (*pb.UploadID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	uploadID := id.String()
	s.uploads[uploadID] = newFileUpload(s.store, s.cfg)

	return &pb.UploadID{Id: uploadID}, nil
}

// AddChunk adds a chunk to a file upload. It expects to receive chunks in sequential
// order. Returns a ResourceExhausted error if its internal work queue waiting for
// chunk data uploads is full. In this case, the request should be retried after more
// chunk data is uploaded. No further chunks should be sent after the chunk with its
// Final field set to true.
func (s *Server) AddChunk(ctx context.Context, c *pb.Chunk) (*pb.Upload, error) {
	upload, err := s.getUpload(c.UploadId)
	if err != nil {
		return nil, err
	}
	upload.Lock()
	defer upload.Unlock()

	if upload.receivedFinal {
		msg := fmt.Sprintf("final chunk receieved at sequence %d", c.Sequence)
		return nil, twirp.NewError(twirp.Aborted, msg)
	}

	// Validate the input
	if c.Size == 0 {
		return nil, twirp.RequiredArgumentError("size")
	}
	if len(c.Sum) == 0 {
		return nil, twirp.RequiredArgumentError("sum")
	}
	next := upload.nextSeq()
	if c.Sequence != next {
		msg := fmt.Sprintf("expected sequence %d but received %d", next, c.Sequence)
		return nil, twirp.NewError(twirp.FailedPrecondition, msg)
	}
	sum, err := sum.FromBytes(c.Sum)
	if err != nil {
		return nil, twirp.InvalidArgumentError("sum", err.Error())
	}
	chunk := object.Chunk{Sequence: c.Sequence, Size: c.Size, Sum: sum}

	// Add the chunk to the packer's work queue if it doesn't already exist in the store
	exists, err := s.db.ChunkExists(sum)
	if err != nil {
		return nil, err
	}
	if !exists {
		select {
		case upload.queue <- packEntry{chunk, c.Final}:
			break
		default:
			return nil, twirp.NewError(twirp.ResourceExhausted, "upload queue full")
		}
	}

	// Close the packer's work queue if this is the final chunk in the file.
	if c.Final {
		close(upload.queue)
		upload.receivedFinal = true
	}

	upload.chunks = append(upload.chunks, chunk)

	return &pb.Upload{Sequence: c.Sequence, Upload: !exists}, nil
}

// Abort cancels a file upload. Any subsequent call to the uploder service referencing
// the upload returns a NotFound error.
func (s *Server) Abort(ctx context.Context, u *pb.UploadID) (*pb.Empty, error) {
	upload, err := s.getUpload(u.Id)
	if err != nil {
		return nil, err
	}
	upload.Lock()
	defer upload.Unlock()
	delete(s.uploads, u.Id)
	return &pb.Empty{}, nil
}

// Complete marks an upload as complete. The caller should not call this method until
// the final chunk has been sent.
func (s *Server) Complete(ctx context.Context, f *pb.File) (*pb.Empty, error) {
	upload, err := s.getUpload(f.UploadId)
	if err != nil {
		return nil, err
	}
	upload.Lock()
	defer upload.Unlock()

	if !upload.receivedFinal {
		return nil, twirp.NewError(twirp.FailedPrecondition, "awaiting final chunk")
	}
	if len(upload.chunks) == 0 {
		return nil, twirp.NewError(twirp.FailedPrecondition, "received no chunks")
	}

	// Wait for the packer to complete
	timer := time.NewTimer(30 * time.Second) // TODO: pass duration as parameter
	select {
	case <-ctx.Done():
		return nil, twirp.NewError(twirp.Canceled, ctx.Err().Error())
	case <-upload.packer.isDone():
		break
	case <-timer.C:
		return nil, twirp.NewError(twirp.DeadlineExceeded, "waiting for upload to complete")
	}

	// Save the File to the store
	file := object.File{Name: f.Name, CreatedAt: time.Now(), Chunks: upload.chunks}
	fBin := file.MarshalBinary()
	fSum := sum.Compute(fBin)
	fKey := server.FileKey(fSum)
	if err = putObject(s.store, s.cfg.Bucket, fKey, fBin); err != nil {
		return nil, fmt.Errorf("saving file %s to store: %w", fKey, err)
	}

	// Save the File to the database
	if err = s.db.InsertFile(file, fSum); err != nil {
		log.OnError(func() error { return s.store.Delete(s.cfg.Bucket, fKey) })
		return nil, fmt.Errorf("inserting file in DB: %w", err)
	}

	delete(s.uploads, f.UploadId)

	return &pb.Empty{}, nil
}

// ChunkUploadHandler is a HTTP request handler that accepts chunk data.
func (s *Server) ChunkUploadHandler(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("upload_id")
	upload, err := s.getUpload(uploadID)
	if herr := toHTTPErr(err); herr != nil {
		http.Error(w, herr.message, herr.code)
		return
	}
	upload.packer.Lock()
	defer upload.packer.Unlock()

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(30*time.Second))
	defer cancel()
	err = upload.packer.loadNextChunk(ctx, r.Body, s.db)
	if herr := toHTTPErr(err); herr != nil {
		http.Error(w, herr.message, herr.code)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) getUpload(id string) (*fileUpload, error) {
	if id == "" {
		return nil, twirp.RequiredArgumentError("upload_id")
	}
	upload, ok := s.uploads[id]
	if !ok {
		return nil, twirp.NotFoundError("upload not found")
	}
	return upload, nil
}

type packEntry struct {
	chunk object.Chunk
	final bool
}

type fileUpload struct {
	sync.Mutex
	chunks        []object.Chunk
	receivedFinal bool
	queue         chan packEntry
	packer        chunkPacker
}

type chunkPacker struct {
	sync.Mutex
	*packer.Packer
	next  *packEntry
	queue <-chan packEntry
	done  chan struct{}
}

func newFileUpload(s store.Store, cfg Config) *fileUpload {
	queue := make(chan packEntry, cfg.QSize)
	return &fileUpload{
		chunks:        make([]object.Chunk, 0),
		receivedFinal: false,
		queue:         queue,
		packer:        newChunkPacker(s, queue, packer.Config{Mode: cfg.Mode, Bucket: cfg.Bucket}),
	}
}

func newChunkPacker(s store.Store, q <-chan packEntry, cfg packer.Config) chunkPacker {
	p := packer.New(s, cfg)
	return chunkPacker{sync.Mutex{}, p, nil, q, make(chan struct{}, 1)}
}

func (u *fileUpload) nextSeq() uint64 {
	if len(u.chunks) == 0 {
		return 0
	}
	return u.chunks[len(u.chunks)-1].Sequence + 1
}

func (p *chunkPacker) loadNextChunk(ctx context.Context, r io.Reader, db *db.Adapter) error {
	if p.next == nil {
		select {
		case e, more := <-p.queue:
			if !more {
				return twirp.NewError(twirp.OutOfRange, "no more chunks requested")
			}
			p.next = &e
		case <-ctx.Done():
			return twirp.NewError(twirp.Canceled, ctx.Err().Error())
		}
	}

	// Load the chunk data from the reader
	c := p.next.chunk
	data := make([]byte, c.Size)
	if n, err := io.ReadFull(r, data); err == io.EOF || err == io.ErrUnexpectedEOF {
		msg := fmt.Sprintf("sequence %d: expected %d bytes but received %d", c.Sequence, c.Size, n)
		return twirp.NewError(twirp.Aborted, msg)
	} else if err != nil {
		return fmt.Errorf("reading chunk data: %w", err)
	}
	s := sum.Compute(data)
	if s != c.Sum {
		msg := fmt.Sprintf(
			"sequence %d: expected checksum %x but received data with checksum %x",
			c.Sequence, c.Sum[:], s[:],
		)
		return twirp.NewError(twirp.Aborted, msg)
	}

	// Flush the packer if it's at capacity or if this is the final chunk
	if p.Size()+c.Size >= maxPackfileSize {
		if err := p.flush(db); err != nil {
			return err
		}
	}

	if err := p.AddChunk(data, s); err != nil {
		return twirp.NewError(twirp.DataLoss, "chunk data lost")
	}

	// Flush the packer if is this is the final chunk and signal completion
	if p.next.final {
		if err := p.flush(db); err != nil {
			return err
		}
		p.done <- struct{}{}
		close(p.done)
	}

	// The chunk has been processed successfully. Reset the next value to nil so we
	// pull a new value off the queue the next time this method is called.
	p.next = nil

	return nil
}

func (p *chunkPacker) flush(db *db.Adapter) error {
	if p.NumChunks() == 0 {
		return nil
	}
	id, index, err := p.Flush()
	if err != nil {
		// Data in the packfile is potentially lost. The client should regard the upload
		// as corrupted.
		// TODO: log this error
		return twirp.NewError(twirp.DataLoss, "chunk data lost")
	}
	if err := db.InsertPackIndex(index, id); err != nil {
		return fmt.Errorf("inserting pack index: %w", err)
	}
	// TODO: insert index into DB
	fmt.Println(id)
	fmt.Println(index.Sum.AsHex())

	return nil
}

func (p *chunkPacker) isDone() <-chan struct{} {
	return p.done
}

type httpError struct {
	message string
	code    int
}

// toHTTPErr converts a twirp error to an httpError with an equivalent HTTP error code,
// or for a non-twirp error, to a HTTP 500 error.
func toHTTPErr(err error) *httpError {
	if err == nil {
		return nil
	}
	if terr, ok := err.(twirp.Error); ok {
		code := twirp.ServerHTTPStatusFromErrorCode(terr.Code())
		return &httpError{err.Error(), code}
	}
	return &httpError{"internal server error", http.StatusInternalServerError}
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
