package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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
func (srv *Server) PackfileUploadHandler(w http.ResponseWriter, req *http.Request) {
	if req.ContentLength <= 0 {
		http.Error(w, "content-length required", http.StatusBadRequest)
		return
	}
	if req.ContentLength > int64(srv.cfg.MaxPackfileSize) {
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
	bucket := srv.cfg.Bucket
	pfile := srv.store.NewFile(bucket, pkey)

	rd := io.TeeReader(io.LimitReader(req.Body, req.ContentLength), pfile)

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
		msg := fmt.Sprintf("provided packfile checksum %x does not match actual checksum %x", sum, index.Sum)
		http.Error(w, msg, http.StatusBadRequest)
	}
	if err = pfile.Close(); err != nil {
		internalError(w, err)
		return
	}

	ikey := digest + ".index"
	b := index.MarshalBinary()
	if err = putObject(srv.store, bucket, ikey, b); err != nil {
		log.OnError(func() error { return srv.store.Delete(bucket, pkey) })
		internalError(w, err)
		return
	}

	if err = srv.db.InsertPackIndex(index, digest); err != nil {
		log.OnError(func() error { return srv.store.Delete(bucket, pkey) })
		log.OnError(func() error { return srv.store.Delete(bucket, ikey) })
		internalError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// CreateFile creates a new file.
func (srv *Server) CreateFile(ctx context.Context, file *pb.File) (*pb.FileID, error) {
	// Ignore leading forward slash on filenames
	name := strings.TrimPrefix(file.Name, "/")
	if name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	chunks := make([]object.Chunk, len(file.Sums))
	for i, s := range file.Sums {
		sum, err := sum.FromBytes(s)
		if err != nil {
			msg := fmt.Sprintf("sum %d: %v", i, err)
			return nil, twirp.InvalidArgumentError("sums", msg)
		}

		size, err := srv.db.GetChunkSize(sum)
		if errors.Is(err, db.ErrNotFound) {
			msg := fmt.Sprintf("sum %d %x does not exist", i, sum)
			return nil, twirp.NewError(twirp.FailedPrecondition, msg)
		} else if err != nil {
			return nil, err
		}

		chunks[i] = object.Chunk{Sequence: uint64(i), Size: size, Sum: sum}
	}

	f := object.File{Name: name, Chunks: chunks, CreatedAt: time.Now().UTC()}
	b := f.MarshalBinary()
	sum := sum.Compute(b)

	fkey := sum.AsHex() + ".file"
	if err := putObject(srv.store, srv.cfg.Bucket, fkey, b); err != nil {
		return nil, err
	}

	if err := srv.db.InsertFile(f, sum); err != nil {
		log.OnError(func() error { return srv.store.Delete(srv.cfg.Bucket, fkey) })
		return nil, err
	}

	return &pb.FileID{Name: file.Name, Sum: sum[:]}, nil
}

// ChunksExist checks if a list of chunks already exist in the store. The response
// contains a boolean for each chunk in the request.
func (srv *Server) ChunksExist(ctx context.Context, req *pb.ChunksExistRequest) (*pb.ChunksExistResponse, error) {
	if len(req.Sums) == 0 {
		return &pb.ChunksExistResponse{Exists: nil}, nil
	}

	sums := make([]sum.Sum, len(req.Sums))
	for i, b := range req.Sums {
		s, err := sum.FromBytes(b)
		if err != nil {
			return nil, twirp.InvalidArgumentError("sums", err.Error())
		}
		sums[i] = s
	}

	exists, err := srv.db.ChunksExist(sums)
	if err != nil {
		return nil, err
	}

	return &pb.ChunksExistResponse{Exists: exists}, nil
}

func internalError(w http.ResponseWriter, e error) {
	http.Error(w, "internal server error", http.StatusInternalServerError)
	log.Error(e)
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

type countingReader struct {
	reader    io.Reader
	bytesRead uint64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.bytesRead += uint64(n)
	return n, err
}
