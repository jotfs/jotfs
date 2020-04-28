package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/object"
	pb "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/sum"

	"github.com/google/uuid"
	"github.com/twitchtv/twirp"
)

// Server implements the FileUploader interface.
type Server struct {
	db      *db.Adapter
	uploads map[string]*fileUpload
	qSize   uint
}

func NewServer(adapter *db.Adapter, qSize uint) (*Server, error) {
	if qSize == 0 {
		return nil, errors.New("queue size must be at least 1")
	}
	return &Server{adapter, make(map[string]*fileUpload), qSize}, nil
}

// Init initializes a new file upload.
func (s *Server) Init(ctx context.Context, _ *pb.Empty) (*pb.UploadID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	uploadID := id.String()
	s.uploads[uploadID] = newFileUpload(s.qSize)

	return &pb.UploadID{Id: uploadID}, nil
}

// AddChunk adds a chunk to a file upload. It expects to receive chunks in sequential
// order. Returns a ResourceExhausted error if its internal work queue waiting for
// chunk data uploads is full. In this case, the request should be retried after more
// chunk data is uploaded.
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
		case upload.queue <- entry{chunk, c.Final}:
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
	case err := <-upload.packer.isDone():
		if err != nil {
			return nil, twirp.NewError(twirp.Aborted, fmt.Sprintf("data upload failed: %v", err))
		}
		break
	case <-timer.C:
		return nil, twirp.NewError(twirp.DeadlineExceeded, "waiting for upload to complete")
	}

	return &pb.Empty{}, nil
}

func (s *Server) ChunkUploadHandler(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("upload_id")
	upload, err := s.getUpload(uploadID)
	if herr := toHTTPErr(err); herr != nil {
		http.Error(w, herr.message, herr.code)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(30*time.Second))
	defer cancel()
	err = upload.packer.loadNextChunk(ctx, r.Body)
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

type entry struct {
	chunk object.Chunk
	final bool
}

type fileUpload struct {
	sync.Mutex
	chunks        []object.Chunk
	receivedFinal bool
	queue         chan entry
	packer        *packer
}

func newFileUpload(qSize uint) *fileUpload {
	queue := make(chan entry, qSize)
	return &fileUpload{
		chunks:        make([]object.Chunk, 0),
		receivedFinal: false,
		queue:         queue,
		packer:        newPacker(queue),
	}
}

func (u *fileUpload) nextSeq() uint64 {
	if len(u.chunks) == 0 {
		return 0
	}
	return u.chunks[len(u.chunks)-1].Sequence + 1
}

type packer struct {
	sync.Mutex
	queue <-chan entry
	done  chan error
	next  *entry
}

func newPacker(queue <-chan entry) *packer {
	return &packer{sync.Mutex{}, queue, make(chan error, 1), nil}
}

func (p *packer) loadNextChunk(ctx context.Context, r io.Reader) error {
	var e entry
	var more bool
	if p.next == nil {
		select {
		case e, more = <-p.queue:
			if !more {
				return twirp.NewError(twirp.OutOfRange, "no more data requested")
			}
		case <-ctx.Done():
			return twirp.NewError(twirp.Canceled, ctx.Err().Error())
		}
	} else {
		e = *p.next
	}

	err := p.processNextChunk(e.chunk, r)
	if err != nil {
		p.next = &e
		return err
	}

	if e.final {
		p.done <- nil
	}

	return nil
}

func (p *packer) processNextChunk(c object.Chunk, r io.Reader) error {
	// Read the next c.Size bytes from r
	data := make([]byte, c.Size)
	if n, err := io.ReadFull(r, data); err == io.ErrUnexpectedEOF {
		msg := fmt.Sprintf("sequence %d: unexpected EOF after %d bytes", c.Sequence, n)
		return twirp.NewError(twirp.Aborted, msg)
	} else if err == io.EOF {
		msg := fmt.Sprintf("sequence %d: no data received", c.Sequence)
		return twirp.NewError(twirp.Aborted, msg)
	} else if err != nil {
		return err
	}

	s := sum.Compute(data)
	if c.Sum != s {
		msg := fmt.Sprintf("sequence %d: expected checksum %x but received %x", c.Sequence, c.Sum, s)
		return twirp.NewError(twirp.Aborted, msg)
	}

	// TODO: write chunk data to packfile builder

	return nil
}

func (p *packer) isDone() <-chan error {
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
