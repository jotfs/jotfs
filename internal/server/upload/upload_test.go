package upload

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/packer"
	pb "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/server/testutil"
	"github.com/iotafs/iotafs/internal/sum"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/twitchtv/twirp"
)

func TestServerInit(t *testing.T) {
	qSize := uint(100)
	db := newTestDB()
	defer db.Close()
	srv := newTestingServer(db, qSize)

	// Init upload
	u, err := srv.Init(context.Background(), &pb.Empty{})
	assert.NoError(t, err)
	assert.NotNil(t, u)
	assert.NotEmpty(t, u.Id)

	// Add a chunk to the upload
	_, c := randChunk(100, 0)
	upload, err := srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
	})
	assert.NoError(t, err)
	assert.NotNil(t, upload)
	assert.Equal(t, upload.Sequence, c.Sequence)
	assert.True(t, upload.Upload)

	// Error if same chunk sequence receieved
	_, err = srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
	})
	assertTwirpError(t, err, twirp.FailedPrecondition)

	// Error if chunk is out of sequence
	_, c = randChunk(100, 999)
	_, err = srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
	})
	assertTwirpError(t, err, twirp.FailedPrecondition)

	// Add final chunk
	_, c = randChunk(100, 1)
	_, err = srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
		Final:    true,
	})
	assert.NoError(t, err)

	// Error if try to add chunk after final
	_, c = randChunk(100, 2)
	_, err = srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
	})
	assertTwirpError(t, err, twirp.Aborted)

	// Abort the upload
	_, err = srv.Abort(context.Background(), u)
	assert.NoError(t, err)

	// Error if abort called twice
	_, err = srv.Abort(context.Background(), u)
	assertTwirpError(t, err, twirp.NotFound)

	// Error if add chunk to aborted upload
	_, err = srv.AddChunk(context.Background(), &pb.Chunk{UploadId: u.Id})
	assertTwirpError(t, err, twirp.NotFound)
}

func TestServerUploadData(t *testing.T) {
	qSize := uint(100)
	db := newTestDB()
	defer db.Close()
	srv := newTestingServer(db, qSize)
	defer srv.db.Close()

	// Add a chunk and close the stream
	data, c := randChunk(100, 0)
	u, err := srv.Init(context.Background(), &pb.Empty{})
	assert.NoError(t, err)
	_, err = srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
		Final:    true,
	})
	assert.NoError(t, err)

	// Upload the chunk data
	url := fmt.Sprintf("http://example.com/?upload_id=%s", u.Id)
	req := httptest.NewRequest("POST", url, bytes.NewReader(data))
	w := httptest.NewRecorder()

	srv.ChunkUploadHandler(w, req)
	resp := w.Result()
	b, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusAccepted)
	assert.Empty(t, b)

	// Complete the upload
	_, err = srv.Complete(context.Background(), &pb.File{UploadId: u.Id, Name: "test.txt"})
	assert.NoError(t, err)
}

func TestPackerBadData(t *testing.T) {
	// Add a chunk to a packer
	adapter := newTestDB()
	defer adapter.Close()
	store := testutil.MockStore{}
	queue := make(chan packEntry)
	packer := newChunkPacker(store, queue, packer.Config{Bucket: "", Mode: compress.None})

	data, c := randChunk(100, 0)
	go func() {
		queue <- packEntry{chunk: c, final: true}
	}()

	// Error if empty data
	var emptyBuf bytes.Buffer
	err := packer.loadNextChunk(context.Background(), &emptyBuf, adapter)
	assertTwirpError(t, err, twirp.Aborted)

	// Error if partial data
	var partialBuf bytes.Buffer
	partialBuf.Write(data[:10])
	err = packer.loadNextChunk(context.Background(), &partialBuf, adapter)
	assertTwirpError(t, err, twirp.Aborted)

	// Error if data with different checksum
	diff, _ := randChunk(100, 0)
	var diffBuf bytes.Buffer
	diffBuf.Write(diff)
	err = packer.loadNextChunk(context.Background(), &diffBuf, adapter)
	assertTwirpError(t, err, twirp.Aborted)
}

func newTestDB() *db.Adapter {
	// id, _ := uuid.NewRandom()
	// file := path.Join(dir, id.String())
	adapter, err := db.Empty()
	if err != nil {
		log.Fatal(err)
	}
	return adapter
}

func newTestingServer(adapter *db.Adapter, qSize uint) *Server {
	// adapter := newTestDB()
	s := testutil.MockStore{}
	srv, err := NewServer(adapter, s, Config{"", qSize, compress.None})
	if err != nil {
		log.Fatal(err)
	}
	return srv
}

func randChunk(size uint64, seq uint64) ([]byte, object.Chunk) {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		log.Fatal(err)
	}
	sum := sum.Compute(data)
	return data, object.Chunk{Sequence: seq, Size: size, Sum: sum}
}

func assertTwirpError(t *testing.T, err error, code twirp.ErrorCode) {
	if err == nil {
		t.Error("expected error but received nil")
		return
	}
	if terr, ok := err.(twirp.Error); ok {
		tcode := terr.Code()
		if tcode != code {
			t.Errorf(
				"expected twirp error code %q but received %q with message: %s",
				code, tcode, terr.Msg(),
			)
		}
		return
	}
	t.Error("expected twirp.Error")
}
