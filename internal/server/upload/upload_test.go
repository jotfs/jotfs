package upload

import (
	"bytes"
	"context"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/object"
	pb "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/server/testutil"
	"github.com/iotafs/iotafs/internal/sum"
	"github.com/twitchtv/twirp"

	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

const maxPackfileSize = 1024 * 1024 * 128

func TestPackfileUploadHandler(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	s := sum.Compute(packfile)

	// Construct the request
	req := httptest.NewRequest("POST", "/packfile", bytes.NewReader(packfile))
	req.Header.Set("x-iota-checksum", base64.StdEncoding.EncodeToString(s[:]))

	w := httptest.NewRecorder()
	srv.PackfileUploadHandler(w, req)
	resp := w.Result()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Empty(t, body)

	// Test ChunksExist
	ctx := context.Background()
	resp2, err := srv.ChunksExist(ctx, &pb.ChunksExistRequest{Sums: [][]byte{aSum[:], bSum[:], aSum[:]}})
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.Equal(t, []bool{true, true, true}, resp2.Exists)

	// Test ChunksExist with empty payload
	resp3, err := srv.ChunksExist(ctx, &pb.ChunksExistRequest{Sums: [][]byte{}})
	assert.NoError(t, err)
	assert.NotNil(t, resp3)
	assert.Nil(t, resp3.Exists)
}

func TestPackfileUploadHandlerBadRequest(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	s := sum.Compute(packfile)

	packfileUploadStatus := func(req *http.Request) int {
		w := httptest.NewRecorder()
		srv.PackfileUploadHandler(w, req)
		resp := w.Result()
		resp.Body.Close()
		return resp.StatusCode
	}

	// Bad content length
	lengths := []int64{0, -1, maxPackfileSize + 1}
	for _, l := range lengths {
		req := httptest.NewRequest("POST", "/packfile", bytes.NewReader(packfile))
		req.Header.Set("x-iota-checksum", base64.StdEncoding.EncodeToString(s[:]))
		req.ContentLength = l
		assert.Equal(t, http.StatusBadRequest, packfileUploadStatus(req))
	}

	// Missing checksum
	req := httptest.NewRequest("POST", "/packfile", bytes.NewReader(packfile))
	assert.Equal(t, http.StatusBadRequest, packfileUploadStatus(req))

	// Checksum does not match
	req = httptest.NewRequest("POST", "/packfile", bytes.NewReader(packfile))
	badSum := make([]byte, sum.Size)
	req.Header.Set("x-iota-checksum", base64.StdEncoding.EncodeToString(badSum))
	assert.Equal(t, http.StatusBadRequest, packfileUploadStatus(req))

	// Corrupted packfile
	req = httptest.NewRequest("POST", "/packfile", bytes.NewReader(packfile[10:]))
	req.Header.Set("x-iota-checksum", base64.StdEncoding.EncodeToString(s[:]))
	assert.Equal(t, http.StatusBadRequest, packfileUploadStatus(req))

}

func TestCreateFile(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	s := sum.Compute(packfile)

	// Upload the packfile so the chunk data exists
	req := httptest.NewRequest("POST", "/packfile", bytes.NewReader(packfile))
	req.Header.Set("x-iota-checksum", base64.StdEncoding.EncodeToString(s[:]))
	w := httptest.NewRecorder()
	srv.PackfileUploadHandler(w, req)
	resp := w.Result()
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create the file
	ctx := context.Background()
	f, err := srv.CreateFile(ctx, &pb.File{
		Name: "test.txt",
		Sums: [][]byte{aSum[:], bSum[:], bSum[:], aSum[:]},
	})
	assert.NoError(t, err)
	assert.NotNil(t, f)

	// Test CreateFile with chunk that don't exist in any packfile
	f, err = srv.CreateFile(ctx, &pb.File{
		Name: "test.txt",
		Sums: [][]byte{aSum[:], bSum[:], make([]byte, sum.Size)},
	})
	assert.Error(t, err)
	assert.Nil(t, f)

	// Test empty CreateFile
	f, err = srv.CreateFile(ctx, &pb.File{
		Name: "test.txt",
		Sums: nil,
	})
	assert.NoError(t, err)
	assert.NotNil(t, f)

	// Test CreateFile with empty name
	ctx = context.Background()
	f, err = srv.CreateFile(ctx, &pb.File{
		Name: "/",
		Sums: [][]byte{aSum[:], bSum[:], bSum[:], aSum[:]},
	})
	assert.Error(t, err)
	assert.Nil(t, f)
}

func TestList(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	uploadPackfile(t, srv, packfile)

	createTestFile(t, "test.txt", srv)
	createTestFile(t, "/data/test2.txt", srv)
	createTestFile(t, "data/test3.doc", srv)

	// List all
	ctx := context.Background()
	resp, err := srv.List(ctx, &pb.ListRequest{Prefix: "/", Limit: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Equal(t, []string{"/data/test3.doc", "/data/test2.txt", "/test.txt"}, getNames(resp.Info))

	// Set limit less than total and ascending order
	resp, err = srv.List(ctx, &pb.ListRequest{Prefix: "/", Limit: 2, Ascending: true})
	assert.NoError(t, err)
	assert.NotEqual(t, int64(-1), resp.NextPageToken)
	assert.Equal(t, []string{"/test.txt", "/data/test2.txt"}, getNames(resp.Info))

	// Get next page
	resp, err = srv.List(ctx, &pb.ListRequest{Prefix: "/", Limit: 2, Ascending: true, NextPageToken: resp.NextPageToken})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Equal(t, []string{"/data/test3.doc"}, getNames(resp.Info))

	// Include / Exclude params
	resp, err = srv.List(ctx, &pb.ListRequest{Prefix: "/", Limit: 10, Exclude: "data/*", Include: "*.doc"})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Equal(t, []string{"/data/test3.doc", "/test.txt"}, getNames(resp.Info))

	// Prefix does not match
	resp, err = srv.List(ctx, &pb.ListRequest{Prefix: "/nomatch", Limit: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Equal(t, []string{}, getNames(resp.Info))

}

func TestHead(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	uploadPackfile(t, srv, packfile)

	// Multiple versions of the same file
	createTestFile(t, "test.txt", srv)
	createTestFile(t, "test.txt", srv)
	createTestFile(t, "test.txt", srv)

	// List all versions
	ctx := context.Background()
	resp, err := srv.Head(ctx, &pb.HeadRequest{Name: "test.txt/", Limit: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Len(t, resp.Info, 3)

	// No matching filename
	resp, err = srv.Head(ctx, &pb.HeadRequest{Name: "does-not-exist", Limit: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Len(t, resp.Info, 0)

	// Test pagination
	resp, err = srv.Head(ctx, &pb.HeadRequest{Name: "test.txt/", Limit: 2})
	assert.NoError(t, err)
	assert.NotEqual(t, int64(-1), resp.NextPageToken)
	assert.Len(t, resp.Info, 2)

	resp, err = srv.Head(ctx, &pb.HeadRequest{Name: "test.txt/", Limit: 2, NextPageToken: resp.NextPageToken})
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), resp.NextPageToken)
	assert.Len(t, resp.Info, 1)
}

func TestDownload(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	uploadPackfile(t, srv, packfile)
	f := createTestFile(t, "test.txt", srv)

	// Get download response
	ctx := context.Background()
	resp, err := srv.Download(ctx, f)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	// Error if file doesn't exist
	resp, err = srv.Download(ctx, &pb.FileID{Sum: make([]byte, sum.Size)})
	assert.Error(t, err)
	assert.Nil(t, resp)
	terr, ok := err.(twirp.Error)
	assert.True(t, ok)
	assert.Equal(t, twirp.NotFound, terr.Code())
}

func TestCopy(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	uploadPackfile(t, srv, packfile)
	f := createTestFile(t, "test.txt", srv)

	// Copy
	ctx := context.Background()
	resp, err := srv.Copy(ctx, &pb.CopyRequest{SrcId: f.Sum, Dst: "/data/test2.txt/"})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	lresp, err := srv.List(ctx, &pb.ListRequest{Prefix: "/data/test2.txt", Limit: 10})
	assert.NoError(t, err)
	assert.Equal(t, []string{"/data/test2.txt"}, getNames(lresp.Info))

	// Error if file does not exist
	resp, err = srv.Copy(ctx, &pb.CopyRequest{SrcId: make([]byte, sum.Size), Dst: "abc"})
	assert.Error(t, err)
	assert.Nil(t, resp)
	terr, ok := err.(twirp.Error)
	assert.True(t, ok)
	assert.Equal(t, twirp.NotFound, terr.Code())
}

func TestDelete(t *testing.T) {
	srv, dbname := testServer(t)
	defer os.Remove(dbname)
	packfile := genTestPackfile(t)
	uploadPackfile(t, srv, packfile)
	f := createTestFile(t, "test.txt", srv)

	// Delete
	ctx := context.Background()
	_, err := srv.Delete(ctx, f)
	assert.NoError(t, err)
	lresp, err := srv.List(ctx, &pb.ListRequest{Prefix: "test.txt", Limit: 10})
	assert.NoError(t, err)
	assert.Empty(t, lresp.Info)

	// Error if file does not exist
	_, err = srv.Delete(ctx, &pb.FileID{Sum: make([]byte, sum.Size)})
	assert.Error(t, err)
	terr, ok := err.(twirp.Error)
	assert.True(t, ok)
	assert.Equal(t, twirp.NotFound, terr.Code())
}

func testServer(t *testing.T) (*Server, string) {
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(os.TempDir(), "iota-"+id.String())
	adapter, err := db.EmptyDisk(name)
	if err != nil {
		t.Fatal(err)
	}
	store := testutil.MockStore{}
	cfg := Config{
		MaxChunkSize:    1024 * 1024 * 8,
		MaxPackfileSize: maxPackfileSize,
	}
	srv := NewServer(adapter, store, cfg)
	return srv, name
}

// genTestPackfile generates a packfile for testing
func genTestPackfile(t *testing.T) []byte {
	buf := new(bytes.Buffer)
	builder, err := object.NewPackfileBuilder(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = builder.Append(a, aSum, compress.None)
	if err != nil {
		t.Fatal(err)
	}
	err = builder.Append(b, bSum, compress.Zstd)
	if err != nil {
		t.Fatal(err)
	}

	d := make([]byte, buf.Len())
	copy(d, buf.Bytes())

	return d
}

func uploadPackfile(t *testing.T, srv *Server, data []byte) {
	s := sum.Compute(data)
	req := httptest.NewRequest("POST", "/packfile", bytes.NewReader(data))
	req.Header.Set("x-iota-checksum", base64.StdEncoding.EncodeToString(s[:]))
	w := httptest.NewRecorder()
	srv.PackfileUploadHandler(w, req)
	resp := w.Result()
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.FailNow()
	}
}

func createTestFile(t *testing.T, name string, srv *Server) *pb.FileID {
	ctx := context.Background()
	f, err := srv.CreateFile(ctx, &pb.File{
		Name: name,
		Sums: [][]byte{aSum[:], bSum[:], bSum[:], aSum[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func getNames(infos []*pb.FileInfo) []string {
	res := make([]string, len(infos))
	for i := range infos {
		res[i] = infos[i].Name
	}
	return res
}

var a = []byte(`A celebrated tenor had sung in Italian, and a notorious contralto had sung 
 in jazz, and between the numbers people were doing "stunts." all over the garden, while 
happy, vacuous bursts of laughter rose toward the summer sky. A pair of stage twins, who 
turned out to be the girls in yellow, did a baby act in costume, and champagne was served 
in glasses bigger than finger-bowls. The moon had risen higher, and floating in the Sound 
was a triangle of silver scales, trembling a little to the stiff, tinny drip of the 
banjoes on the lawn.`)

var aSum = sum.Compute(a)

var b = []byte(`And as I sat there brooding on the old, unknown world, I thought of Gatsby’s 
wonder when he first picked out the green light at the end of Daisy’s dock. He had come 
a long way to this blue lawn, and his dream must have seemed so close that he could 
hardly fail to grasp it. He did not know that it was already behind him, somewhere back 
in that vast obscurity beyond the city, where the dark fields of the republic rolled on 
under the night. Gatsby believed in the green light, the orgastic future that year by 
year recedes before us. It eluded us then, but that’s no matter -- tomorrow we will run 
faster, stretch out our arms farther... And one fine morning -- So we beat on, boats 
against the current, borne back ceaselessly into the past.`)

var bSum = sum.Compute(b)
