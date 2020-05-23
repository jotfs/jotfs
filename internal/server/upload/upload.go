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
	name := file.Name
	if name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
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

	return &pb.FileID{Sum: sum[:]}, nil
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

func (srv *Server) ListFiles(ctx context.Context, p *pb.Prefix) (*pb.Files, error) {
	prefix := p.Prefix
	if prefix == "" {
		return nil, twirp.RequiredArgumentError("prefix")
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	infos, err := srv.db.ListFiles(prefix, 1000)
	if err != nil {
		return nil, err
	}

	res := make([]*pb.FileInfo, len(infos))
	for i := range infos {
		info := infos[i]  // don't use range value
		res[i] = &pb.FileInfo{
			Name:      info.Name,
			CreatedAt: info.CreatedAt.UnixNano(),
			Size:      info.Size,
			Sum:       info.Sum[:],
		}
	}

	return &pb.Files{Infos: res}, nil
}

func (srv *Server) HeadFile(ctx context.Context, req *pb.HeadFileRequest) (*pb.HeadFileResponse, error) {
	name := req.Name
	if name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	if req.Limit == 0 {
		return nil, twirp.RequiredArgumentError("limit")
	}
	if req.Limit > 10000 {
		return nil, twirp.InvalidArgumentError("limit", "max is 10000")
	}
	if req.NextPageToken < 0 {
		return nil, twirp.InvalidArgumentError("next_page_token", "cannot be negative")
	}

	versions, err := srv.db.GetFileVersions(name, req.NextPageToken, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("db GetFileVersions: %w", err)
	}

	res := make([]*pb.FileInfo, len(versions))
	for i := range versions {
		info := versions[i]  // don't use range value
		res[i] = &pb.FileInfo{
			Name:      info.Name,
			CreatedAt: info.CreatedAt.UnixNano(),
			Size:      info.Size,
			Sum:       info.Sum[:],
		}
	}

	nextToken := int64(-1)
	if uint64(len(res)) == req.Limit && len(res) > 0 {
		nextToken = res[len(res)-1].CreatedAt
	}

	return &pb.HeadFileResponse{Info: res, NextPageToken: nextToken}, nil
}

type chunk struct {
	Sequence    uint64
	Sum         sum.Sum
	Size        uint64
	BlockOffset uint64
}

type section struct {
	chunks  []chunk
	packSum sum.Sum
	start   uint64
	end     uint64
}

func (srv *Server) Download(ctx context.Context, id *pb.FileID) (*pb.DownloadResponse, error) {
	if id.Sum == nil {
		return nil, twirp.RequiredArgumentError("sum")
	}
	fileID, err := sum.FromBytes(id.Sum)
	if err != nil {
		return nil, twirp.InvalidArgumentError("sum", err.Error())
	}

	indices, err := srv.db.GetFileChunks(fileID)
	if errors.Is(err, db.ErrNotFound) {
		return nil, twirp.NotFoundError(fmt.Sprintf("file %x", id.Sum))
	}
	if err != nil {
		return nil, fmt.Errorf("db GetFileChunks: %w", err)
	}

	// Gather the chunks into sections corresponding to contiguous slices of a packfile
	sections := make([]section, 0)
	var packSum sum.Sum
	var blockStart object.BlockInfo
	var blockEnd object.BlockInfo
	var chunks []chunk
	for i, idx := range indices {
		bseq := idx.Block.Sequence
		if packSum == idx.PackSum && bseq >= blockStart.Sequence && bseq <= blockEnd.Sequence+1 {
			if bseq == blockEnd.Sequence+1 {
				blockEnd = idx.Block
			}
		} else {
			// New section
			if i != 0 {
				sections = append(sections, section{
					chunks:  chunks,
					packSum: packSum,
					start:   blockStart.Offset,
					end:     blockEnd.Offset + blockEnd.Size,
				})
			}

			packSum = idx.PackSum
			blockStart = idx.Block
			blockEnd = idx.Block
			chunks = nil
		}
		chunks = append(chunks, chunk{
			Sequence:    idx.Sequence,
			Sum:         idx.Block.Sum,
			Size:        idx.Block.ChunkSize,
			BlockOffset: idx.Block.Offset - blockStart.Offset,
		})
	}
	sections = append(sections, section{ // Don't forget the final section
		chunks:  chunks,
		packSum: packSum,
		start:   blockStart.Offset,
		end:     blockEnd.Offset + blockEnd.Size,
	})

	// Generate a pre-signed URL to download the data for each section
	urls := make([]string, len(sections))
	bucket := srv.cfg.Bucket
	for i, section := range sections {
		key := section.packSum.AsHex() + ".pack"
		expires := time.Duration(15 * time.Minute)
		rnge := &store.Range{From: section.start, To: section.end}
		url, err := srv.store.PresignGetURL(bucket, key, expires, rnge)
		if err != nil {
			return nil, err
		}
		urls[i] = url
	}

	// Constuct the response
	rSections := make([]*pb.Section, len(sections))
	for i, section := range sections {
		rChunks := make([]*pb.SectionChunk, len(section.chunks))
		for j, chunk := range section.chunks {
			rChunks[j] = &pb.SectionChunk{
				Sequence:    chunk.Sequence,
				Size:        chunk.Size,
				Sum:         chunk.Sum[:],
				BlockOffset: chunk.BlockOffset,
			}
		}
		rSections[i] = &pb.Section{
			Chunks:     rChunks,
			Url:        urls[i],
			RangeStart: section.start,
			RangeEnd:   section.end,
		}
	}
	resp := &pb.DownloadResponse{Sections: rSections}

	return resp, nil

}

func (srv *Server) Copy(ctx context.Context, req *pb.CopyRequest) (*pb.FileID, error) {
	if req.SrcId == nil {
		return nil, twirp.RequiredArgumentError("src_id")
	}
	dst := req.Dst
	if dst == "" {
		return nil, twirp.RequiredArgumentError("dst")
	}
	if !strings.HasPrefix(dst, "/") {
		dst = "/" + dst
	}
	srcID, err := sum.FromBytes(req.SrcId)
	if err != nil {
		return nil, twirp.InvalidArgumentError("src_id", err.Error())
	}

	// Get the file
	f, err := srv.db.GetFile(srcID)
	if errors.Is(err, db.ErrNotFound) {
		return nil, twirp.NotFoundError(fmt.Sprintf("file %x", srcID))
	} else if err != nil {
		return nil, fmt.Errorf("db GetFile: %w", err)
	}
	f.Name = dst
	f.CreatedAt = time.Now().UTC()

	// Save the new file to the database and store
	b := f.MarshalBinary()
	sum := sum.Compute(b)

	fkey := sum.AsHex() + ".file"
	if err := putObject(srv.store, srv.cfg.Bucket, fkey, b); err != nil {
		return nil, err
	}

	if err := srv.db.InsertFile(f, sum); err != nil {
		log.OnError(func() error { return srv.store.Delete(srv.cfg.Bucket, fkey) })
		return nil, fmt.Errorf("inserting file: %w", err)
	}

	return &pb.FileID{Sum: sum[:]}, nil
}

func (srv *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.FileID, error) {
	return nil, nil
}

func (srv *Server) Delete(ctx context.Context, fileID *pb.FileID) (*pb.Empty, error) {
	if fileID.Sum == nil {
		return nil, twirp.RequiredArgumentError("sum")
	}
	s, err := sum.FromBytes(fileID.Sum)
	if err != nil {
		return nil, twirp.InvalidArgumentError("sum", err.Error())
	}

	if _, err = srv.db.GetFileInfo(s); errors.Is(err, db.ErrNotFound) {
		return nil, twirp.NotFoundError(fmt.Sprintf("file %x", s))
	} else if err != nil {
		return nil, fmt.Errorf("db GetFileInfo: %w", err)
	}

	key := s.AsHex() + ".file"
	if err := srv.store.Delete(srv.cfg.Bucket, key); err != nil {
		return nil, fmt.Errorf("deleting file %s from store: %w", key, err)
	}

	if err := srv.db.DeleteFile(s); err != nil {
		return nil, fmt.Errorf("db DeleteFile: %w", err)
	}

	return &pb.Empty{}, nil
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
