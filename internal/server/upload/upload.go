package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/log"
	"github.com/iotafs/iotafs/internal/object"
	pb "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/iotafs/iotafs/internal/sum"
	"github.com/twitchtv/twirp"
)

// Config stores the configuration for the Server.
type Config struct {
	// Bucket is the bucket the server saves files to.
	Bucket string

	// MaxChunkSize is the maximum permitted size of a chunk in bytes.
	MaxChunkSize uint64

	// MaxPackfileSize is the maximum permitted size of a packfile in bytes.
	MaxPackfileSize uint64
}

// Server implements the Api interface specified in upload.proto.
type Server struct {
	db    *db.Adapter
	store store.Store
	cfg   Config
}

// NewServer creates a new Server.
func NewServer(db *db.Adapter, s store.Store, cfg Config) *Server {
	return &Server{
		db:    db,
		cfg:   cfg,
		store: s,
	}
}

// PackfileUploadHandler accepts a Packfile from a client and saves it to the store.
func (s *Server) PackfileUploadHandler(w http.ResponseWriter, req *http.Request) {
	length, err := getContentLength(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if length > s.cfg.MaxPackfileSize {
		http.Error(w, "content-length exceeds maximum packfile size", http.StatusBadRequest)
		return
	}

	sum, err := getChecksum(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	digest := sum.AsHex()
	pkey := digest + ".pack"
	bucket := s.cfg.Bucket
	pfile := s.store.NewFile(bucket, pkey)

	rd := io.LimitReader(req.Body, int64(length))
	rd = io.TeeReader(rd, pfile)

	index, err := object.LoadPackIndex(rd)
	if err != nil {
		// TODO: a write error will appear as a read error here because we're using a
		// TeeReader. Need to distinguish between a malformed client packfile, and a
		// write failure to the store which should be a http.InternalServerError.
		log.OnError(pfile.Cancel)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if index.Sum != sum {
		log.OnError(pfile.Cancel)
		msg := fmt.Sprintf("provided checksum %x does not match actual checksum %x", sum, index.Sum)
		http.Error(w, msg, http.StatusBadRequest)
	}
	if err = pfile.Close(); err != nil {
		internalError(w, err)
		return
	}

	ikey := digest + ".index"
	b := index.MarshalBinary()
	if err = putObject(s.store, bucket, ikey, b); err != nil {
		log.OnError(func() error { return s.store.Delete(bucket, pkey) })
		internalError(w, err)
		return
	}

	if err = s.db.InsertPackIndex(index, digest); err != nil {
		log.OnError(func() error { return s.store.Delete(bucket, pkey) })
		log.OnError(func() error { return s.store.Delete(bucket, ikey) })
		internalError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// CreateFile creates a new file.
func (s *Server) CreateFile(ctx context.Context, file *pb.File) (*pb.FileID, error) {

	chunks := make([]object.Chunk, len(file.Chunks))
	for i, c := range file.Chunks {
		s, err := sum.FromBytes(c.Sum)
		if err != nil {
			msg := fmt.Sprintf("chunk %d: invalid checksum: %v", c.Sequence, err)
			return nil, twirp.InvalidArgumentError("file", msg)
		}
		chunks[i] = object.Chunk{Sequence: c.Sequence, Size: c.Size, Sum: s}
	}

	f := object.File{Name: file.Name, Chunks: chunks, CreatedAt: time.Now().UTC()}
	b := f.MarshalBinary()
	sum := sum.Compute(b)

	fkey := sum.AsHex() + ".file"
	if err := putObject(s.store, s.cfg.Bucket, fkey, b); err != nil {
		return nil, err
	}

	if err := s.db.InsertFile(f, sum); err != nil {
		log.OnError(func() error { return s.store.Delete(s.cfg.Bucket, fkey) })
		return nil, err
	}

	return &pb.FileID{Name: file.Name, Sum: sum[:]}, nil
}

// ChunksExist checks if a list of chunks already exist in the store. The response
// contains a boolean for each chunk in the request.
func (s *Server) ChunksExist(ctx context.Context, req *pb.ChunksExistRequest) (*pb.ChunksExistResponse, error) {

	sums := make([]sum.Sum, len(req.Sums))
	for i, b := range req.Sums {
		s, err := sum.FromBytes(b)
		if err != nil {
			return nil, twirp.InvalidArgumentError("sums", err.Error())
		}
		sums[i] = s
	}

	exists, err := s.db.ChunksExist(sums)
	if err != nil {
		return nil, err
	}

	return &pb.ChunksExistResponse{Exists: exists}, nil
}

func internalError(w http.ResponseWriter, e error) {
	http.Error(w, "internal server error", http.StatusInternalServerError)
	log.Error(e)
}

func getContentLength(req *http.Request) (uint64, error) {
	h := req.Header.Get("content-length")
	if h == "" {
		return 0, errors.New("content-length required")
	}
	length, err := strconv.ParseUint(h, 10, 64)
	if err != nil || length == 0 {
		return 0, errors.New("invalid content-length")
	}
	return length, nil
}

func getChecksum(req *http.Request) (sum.Sum, error) {
	h := req.Header.Get("x-iota-checksum")
	if h == "" {
		return sum.Sum{}, errors.New("x-iota-checksum required")
	}
	s, err := sum.FromBase64(h)
	if err != nil {
		return sum.Sum{}, fmt.Errorf("invalid x-iota-checksum: %v", err)
	}
	return s, nil
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
