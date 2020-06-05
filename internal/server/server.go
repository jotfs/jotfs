package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/errgroup"

	"github.com/jotfs/jotfs/internal/db"
	"github.com/jotfs/jotfs/internal/log"
	"github.com/jotfs/jotfs/internal/object"
	pb "github.com/jotfs/jotfs/internal/protos"
	"github.com/jotfs/jotfs/internal/store"
	"github.com/jotfs/jotfs/internal/sum"
)

const maxFilenameSize = 1024

const (
	stateNotVacuuming int32 = iota
	stateVacuuming
)

// Config stores the configuration for the Server.
type Config struct {
	// Bucket is the bucket the server saves files to.
	Bucket string

	// VersioningEnabled, if set to true, turns on file versioning.
	VersioningEnabled bool

	// MaxChunkSize is the maximum permitted size of a chunk in bytes.
	MaxChunkSize uint64

	// MaxPackfileSize is the maximum permitted size of a packfile in bytes.
	MaxPackfileSize uint64

	Params ChunkerParams
}

// ChunkerParams store the parameters that should be used to chunk files for a server.
type ChunkerParams struct {
	MinChunkSize  uint `toml:"min_chunk_size"`
	AvgChunkSize  uint `toml:"avg_chunk_size"`
	MaxChunkSize  uint `toml:"max_chunk_size"`
	Normalization uint `toml:"normalization"`
}

// Server implements the Api interface specified in upload.proto.
type Server struct {
	db          *db.Adapter
	store       store.Store
	cfg         Config
	logger      zerolog.Logger
	isVacuuming int32
}

// New creates a new Server.
func New(db *db.Adapter, s store.Store, cfg Config) *Server {
	logger := zerolog.New(ioutil.Discard).Level(zerolog.Disabled)
	return &Server{db: db, cfg: cfg, store: s, logger: logger}
}

// SetLogger sets the logger for the server.
func (srv *Server) SetLogger(logger zerolog.Logger) {
	srv.logger = logger
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

	h := req.Header.Get("x-jotfs-checksum")
	if h == "" {
		http.Error(w, "x-jotfs-checksum required", http.StatusBadRequest)
		return
	}
	sum, err := sum.FromBase64(h)
	if err != nil {
		msg := fmt.Sprintf("invalid x-jotfs-checksum: %v", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	digest := sum.AsHex()
	pkey := digest + ".pack"
	bucket := srv.cfg.Bucket

	// Launch a background goroutine to upload the packfile to the store as it's being
	// validated down below
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	r, pfile := io.Pipe()
	var g errgroup.Group
	g.Go(func() error {
		err := srv.store.Put(ctx, bucket, pkey, r)
		return mergeErrors(err, r.CloseWithError(err))
	})

	rd := io.TeeReader(io.LimitReader(req.Body, req.ContentLength), pfile)

	index, err := object.LoadPackIndex(rd)
	if err != nil {
		// TODO: a write error will appear as a read error here because we're using a
		// TeeReader. Need to distinguish between a malformed client packfile, and a
		// write failure to the store which should be a http.InternalServerError.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if index.Sum != sum {
		msg := fmt.Sprintf("provided packfile checksum %x does not match actual checksum %x", sum, index.Sum)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Close the write side of the pipe so the read side will EOF and the upload goroutine
	// will terminate
	if err = pfile.Close(); err != nil {
		err = fmt.Errorf("closing packfile writer: %w", err)
		internalError(w, err)
		return
	}

	if err = g.Wait(); err != nil {
		err = fmt.Errorf("uploading packfile to store: %w", err)
		internalError(w, err)
		return
	}

	ikey := digest + ".index"
	b := index.MarshalBinary()
	if err = srv.store.Put(ctx, bucket, ikey, bytes.NewReader(b)); err != nil {
		err = mergeErrors(err, srv.store.Delete(bucket, pkey))
		internalError(w, err)
		return
	}

	createdAt := time.Now().UTC()
	if err = srv.db.InsertPackIndex(index, createdAt); err != nil {
		err = mergeErrors(err, srv.store.Delete(bucket, pkey))
		err = mergeErrors(err, srv.store.Delete(bucket, ikey))
		internalError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// CreateFile creates a new file. Returns an error if any chunk referenced by the file
// does not exist.
func (srv *Server) CreateFile(ctx context.Context, file *pb.File) (*pb.FileID, error) {
	name := file.Name
	if name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}
	name = cleanFilename(name)
	if err := validateFilename(name); err != nil {
		return nil, twirp.InvalidArgumentError("name", err.Error())
	}

	// Check if this file has a previous version
	var hasPrev bool
	prevInfo, err := srv.db.GetLatestFileVersion(name)
	if errors.Is(err, db.ErrNotFound) {
		hasPrev = false
	} else if err != nil {
		return nil, fmt.Errorf("db GetLatestFileVersion: %w", err)
	} else {
		hasPrev = true
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

	f := object.File{Name: name, Chunks: chunks, CreatedAt: time.Now().UTC(), Versioned: srv.cfg.VersioningEnabled}
	b := f.MarshalBinary()
	sum := sum.Compute(b)

	fkey := sum.AsHex() + ".file"
	if err := srv.store.Put(ctx, srv.cfg.Bucket, fkey, bytes.NewReader(b)); err != nil {
		return nil, err
	}

	if err := srv.db.InsertFile(f, sum); err != nil {
		err = mergeErrors(err, srv.store.Delete(srv.cfg.Bucket, fkey))
		return nil, err
	}

	// Delete the previous version if versioning is turned off
	if hasPrev && !prevInfo.Versioned && !srv.cfg.VersioningEnabled {
		if _, err = srv.Delete(ctx, &pb.FileID{Sum: prevInfo.Sum[:]}); err != nil {
			log.Error(err)
		}
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
	for i := range req.Sums {
		s, err := sum.FromBytes(req.Sums[i])
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

// List returns all versions of files with a given prefix. The response NextPageToken can
// be used to retrieve the next page of results, unless it has the value -1, in which case
// no further pages exist. The parameter Limit sets the maximum number of results per
// page and must be provided. The parameter Exclude may be provided as a glob pattern to
// exclude files from the response. If Exclude is set, the Include parameter may also
// be provided to force inclusion of any files excluded by the Exclude pattern. Results
// are returned in reverse-chronological order of file created date by default. Ascending
// may be set to true to reverse the order.
func (srv *Server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	prefix := req.Prefix
	if prefix == "" {
		return nil, twirp.RequiredArgumentError("prefix")
	}
	prefix = cleanFilename(prefix)
	if req.Limit == 0 {
		return nil, twirp.RequiredArgumentError("limit")
	}
	if req.Limit > 10000 {
		return nil, twirp.InvalidArgumentError("limit", "max is 10000")
	}
	if req.NextPageToken < 0 {
		return nil, twirp.InvalidArgumentError("next_page_token", "cannot be negative")
	}

	exclude := cleanFilename(req.Exclude)
	include := cleanFilename(req.Include)
	infos, err := srv.db.ListFiles(prefix, req.NextPageToken, req.Limit, exclude, include, req.Ascending)
	if err != nil {
		return nil, err
	}

	res := make([]*pb.FileInfo, len(infos))
	for i := range infos {
		info := infos[i] // don't use range value
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

	return &pb.ListResponse{Info: res, NextPageToken: nextToken}, nil
}

// Head returns all versions of a file with a given name. The parameters Limit,
// NextPageToken and Ascending have the same meaning as in the List method.
func (srv *Server) Head(ctx context.Context, req *pb.HeadRequest) (*pb.HeadResponse, error) {
	name := req.Name
	if name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}
	name = cleanFilename(name)
	if req.Limit == 0 {
		return nil, twirp.RequiredArgumentError("limit")
	}
	if req.Limit > 10000 {
		return nil, twirp.InvalidArgumentError("limit", "max is 10000")
	}
	if req.NextPageToken < 0 {
		return nil, twirp.InvalidArgumentError("next_page_token", "cannot be negative")
	}

	versions, err := srv.db.GetFileVersions(name, req.NextPageToken, req.Limit, req.Ascending)
	if err != nil {
		return nil, fmt.Errorf("db GetFileVersions: %w", err)
	}

	res := make([]*pb.FileInfo, len(versions))
	for i := range versions {
		info := versions[i] // don't use range value
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

	return &pb.HeadResponse{Info: res, NextPageToken: nextToken}, nil
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

// Download returns a collection of URLs to download the data for a file. Each URL
// contains data for a contiguous section of the file.
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
					end:     blockEnd.Offset + blockEnd.Size - 1,
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
		end:     blockEnd.Offset + blockEnd.Size - 1,
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
		section := section
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

// Copy makes a copy of a file and returns its ID. Returns a NotFound error if the file
// does not exist.
func (srv *Server) Copy(ctx context.Context, req *pb.CopyRequest) (*pb.FileID, error) {
	if req.SrcId == nil {
		return nil, twirp.RequiredArgumentError("src_id")
	}
	dst := req.Dst
	if dst == "" {
		return nil, twirp.RequiredArgumentError("dst")
	}
	dst = cleanFilename(dst)
	if err := validateFilename(dst); err != nil {
		return nil, twirp.InvalidArgumentError("dst", err.Error())
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
	if err := srv.store.Put(ctx, srv.cfg.Bucket, fkey, bytes.NewReader(b)); err != nil {
		return nil, err
	}

	if err := srv.db.InsertFile(f, sum); err != nil {
		err = mergeErrors(err, srv.store.Delete(srv.cfg.Bucket, fkey))
		return nil, fmt.Errorf("inserting file: %w", err)
	}

	return &pb.FileID{Sum: sum[:]}, nil
}

// Delete removes a file. Returns a NotFound error if the files does not exist.
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

// GetChunkerParams returns the chunking parameters that clients should use to chunk
// files for this server.
func (srv *Server) GetChunkerParams(ctx context.Context, _ *pb.Empty) (*pb.ChunkerParams, error) {
	p := srv.cfg.Params
	return &pb.ChunkerParams{
		MinChunkSize:  uint64(p.MinChunkSize),
		AvgChunkSize:  uint64(p.AvgChunkSize),
		MaxChunkSize:  uint64(p.MaxChunkSize),
		Normalization: uint64(p.Normalization),
	}, nil
}

// StartVacuum starts a new vacuum process. Returns a twirp.Unavailable error if
// a vacuum process is already running. Returns an ID for the vacuum which can be used
// to check the status of the vacuum.
func (srv *Server) StartVacuum(ctx context.Context, _ *pb.Empty) (*pb.VacuumID, error) {
	if !atomic.CompareAndSwapInt32(&srv.isVacuuming, stateNotVacuuming, stateVacuuming) {
		return nil, twirp.NewError(twirp.Unavailable, "vacuum already in progress")
	}
	id, err := srv.db.InsertVacuum(time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("db InsertVacuum: %v", err)
	}
	go func() {
		defer atomic.StoreInt32(&srv.isVacuuming, stateNotVacuuming)
		// Don't use the request context because it will be cancelled when the parent
		// returns
		ctx := context.Background()

		srv.logger.Info().Str("id", id).Msg("Manual vacuum initiated")
		start := time.Now()

		err := srv.runVacuum(ctx, time.Now())
		if err != nil {
			srv.logger.Error().Msgf("vacuum failed: %v", err)
			if err = srv.db.UpdateVacuum(id, time.Now().UTC(), db.VacuumFailed); err != nil {
				srv.logger.Error().Str("id", id).Msg(err.Error())
			}
			return
		}
		if err = srv.db.UpdateVacuum(id, time.Now().UTC(), db.VacuumOK); err != nil {
			srv.logger.Error().Msg(err.Error())
		}

		elapsed := time.Since(start).Milliseconds()
		srv.logger.Info().Str("id", id).Int64("elapsed", elapsed).Msg("Vacuum complete")
	}()
	return &pb.VacuumID{Id: id}, nil
}

// VacuumStatus returns the status of a vacuum process with a given ID. Returns a
// twirp.NotFound error if the vacuum does not exist.
func (srv *Server) VacuumStatus(ctx context.Context, id *pb.VacuumID) (*pb.Vacuum, error) {
	vacuum, err := srv.db.GetVacuum(id.Id)
	if errors.Is(err, db.ErrNotFound) {
		return nil, twirp.NotFoundError(fmt.Sprintf("vacuum %s", id.Id))
	}
	if err != nil {
		return nil, fmt.Errorf("db GetVacuum: %w", err)
	}
	return &pb.Vacuum{
		Status:      vacuum.Status.String(),
		StartedAt:   vacuum.StartedAt,
		CompletedAt: vacuum.CompletedAt,
	}, nil
}

// ServerStats returns summary statistics for the server.
func (srv *Server) ServerStats(ctx context.Context, _ *pb.Empty) (*pb.Stats, error) {
	stats, err := srv.db.GetServerStats()
	if err != nil {
		return nil, fmt.Errorf("db GetServerStats: %w", err)
	}
	return &pb.Stats{
		NumFiles:        stats.NumFiles,
		NumFileVersions: stats.NumFileVersions,
		TotalFilesSize:  stats.TotalFilesSize,
		TotalDataSize:   stats.TotalDataSize,
	}, nil
}

// internalError writes a generic internal server error message to a HTTP response, and
// logs the actual error.
func internalError(w http.ResponseWriter, e error) {
	http.Error(w, "internal server error", http.StatusInternalServerError)
	log.Error(e)
}

// cleanFilename processes a filename to be stored in the database. Trailing slashes are
// removed and a leading slash is prefixed if not already present.
func cleanFilename(name string) string {
	if name == "" {
		return name
	}
	name = strings.TrimSpace(name)
	name = path.Clean(name)
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	if strings.HasSuffix(name, "/") {
		name = strings.TrimSuffix(name, "/")
	}
	return name
}

// validateFilename returns an error if the file name is invalid.
func validateFilename(name string) error {
	if len(name) > maxFilenameSize {
		return fmt.Errorf("filename exceeds maximum size %d", maxFilenameSize)
	}
	if name == "" || name == "/" {
		return errors.New("invalid filename")
	}
	return nil
}

func mergeErrors(err, minor error) error {
	if err == nil && minor == nil {
		return nil
	}
	if minor == nil {
		return err
	}
	if err == nil {
		return minor
	}
	return fmt.Errorf("%w; %v", err, minor)
}
