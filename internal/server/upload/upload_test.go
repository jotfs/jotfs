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

	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/object"
	pb "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/sum"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/twitchtv/twirp"
)

func TestServerInit(t *testing.T) {
	qSize := uint(100)
	srv := newTestingServer(qSize)

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
	srv := newTestingServer(qSize)

	// Add a chunk and close the stream
	data, c := randChunk(100, 0)
	u, _ := srv.Init(context.Background(), &pb.Empty{})
	srv.AddChunk(context.Background(), &pb.Chunk{
		UploadId: u.Id,
		Sequence: c.Sequence,
		Size:     c.Size,
		Sum:      c.Sum[:],
		Final:    true,
	})

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
	queue := make(chan entry)
	packer := newPacker(queue)

	data, c := randChunk(100, 0)
	go func() {
		queue <- entry{chunk: c, final: true}
	}()

	// Error if empty data
	var emptyBuf bytes.Buffer
	err := packer.loadNextChunk(context.Background(), &emptyBuf)
	assertTwirpError(t, err, twirp.Aborted)
	assert.Contains(t, err.Error(), "no data")

	// Error if partial data
	var partialBuf bytes.Buffer
	partialBuf.Write(data[:10])
	err = packer.loadNextChunk(context.Background(), &partialBuf)
	assertTwirpError(t, err, twirp.Aborted)
	assert.Contains(t, err.Error(), "unexpected EOF")

	// Error if data with different checksum
	diff, _ := randChunk(100, 0)
	var diffBuf bytes.Buffer
	diffBuf.Write(diff)
	err = packer.loadNextChunk(context.Background(), &diffBuf)
	assertTwirpError(t, err, twirp.Aborted)
	assert.Contains(t, err.Error(), "expected checksum")
}

func newTestingServer(qSize uint) *Server {
	adapter, err := db.Empty()
	if err != nil {
		log.Fatal(err)
	}
	srv, err := NewServer(adapter, qSize)
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
