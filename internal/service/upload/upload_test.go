package upload

import (
	"bytes"
	"log"
	"math/rand"
	"testing"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/db/testdb"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/service"
	"github.com/iotafs/iotafs/internal/sum"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	store := service.MockStore{}
	cfg := Config{Mode: compress.None, Bucket: "test"}
	db, err := testdb.Empty()
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer db.Close()

	svc := New(cfg, db, store)
	assert.NotNil(t, svc)

	d0 := []byte("hello")
	d1 := []byte("world")

	c0 := object.Chunk{Sequence: 0, Size: uint64(len(d0)), Sum: sum.Compute(d0)}
	c1 := object.Chunk{Sequence: 1, Size: uint64(len(d1)), Sum: sum.Compute(d1)}
	file := object.File{Name: "test.txt", Chunks: []object.Chunk{c0, c1}}
	res, err := svc.CreateFile(file)
	assert.NoError(t, err)

	pbs := []PackBlueprint{
		{Size: c0.Size + c1.Size, Chunks: []object.Chunk{c0, c1}},
	}
	assert.Equal(t, pbs, res.Blueprints)

	// Test upload pack
	var buf bytes.Buffer
	buf.Write(d0)
	buf.Write(d1)
	svc.UploadPack(res.UploadID, 0, &buf)
}

func randSum() sum.Sum {
	s := sum.Sum{}
	rand.Read(s[:])
	return s
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
