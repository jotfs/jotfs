package upload

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/server/testutil"
	"github.com/iotafs/iotafs/internal/sum"
	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

func TestPackfileUploadHandler(t *testing.T) {
	srv := testServer(t)
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
}

func testServer(t *testing.T) *Server {
	adapter, err := db.Empty()
	if err != nil {
		t.Fatal(err)
	}
	store := testutil.MockStore{}
	cfg := Config{
		MaxChunkSize:    1024 * 1024 * 8,
		MaxPackfileSize: 1024 * 1024 * 128,
	}
	srv := NewServer(adapter, store, cfg)
	return srv
}

// genTestPackfile generates a packfile for testing
func genTestPackfile(t *testing.T) []byte {

	a := []byte(`A celebrated tenor had sung in Italian, and a notorious contralto had sung 
 in jazz, and between the numbers people were doing "stunts." all over the garden, while 
happy, vacuous bursts of laughter rose toward the summer sky. A pair of stage twins, who 
turned out to be the girls in yellow, did a baby act in costume, and champagne was served 
in glasses bigger than finger-bowls. The moon had risen higher, and floating in the Sound 
was a triangle of silver scales, trembling a little to the stiff, tinny drip of the 
banjoes on the lawn.`)

	b := []byte(`And as I sat there brooding on the old, unknown world, I thought of Gatsby’s 
wonder when he first picked out the green light at the end of Daisy’s dock. He had come 
a long way to this blue lawn, and his dream must have seemed so close that he could 
hardly fail to grasp it. He did not know that it was already behind him, somewhere back 
in that vast obscurity beyond the city, where the dark fields of the republic rolled on 
under the night. Gatsby believed in the green light, the orgastic future that year by 
year recedes before us. It eluded us then, but that’s no matter -- tomorrow we will run 
faster, stretch out our arms farther... And one fine morning -- So we beat on, boats 
against the current, borne back ceaselessly into the past.`)

	buf := new(bytes.Buffer)
	builder, err := object.NewPackfileBuilder(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = builder.Append(a, sum.Compute(a), compress.None)
	if err != nil {
		t.Fatal(err)
	}
	err = builder.Append(b, sum.Compute(b), compress.Zstd)
	if err != nil {
		t.Fatal(err)
	}

	d := make([]byte, buf.Len())
	copy(d, buf.Bytes())

	return d

}
