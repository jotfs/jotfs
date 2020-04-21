package upload

import (
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

	c0 := object.Chunk{Sequence: 0, Size: 100, Sum: randSum()}
	c1 := object.Chunk{Sequence: 1, Size: 100, Sum: randSum()}
	file := object.File{Name: "test.txt", Chunks: []object.Chunk{c0, c1}}
	res, err := svc.CreateFile(file)
	assert.NoError(t, err)

	pb := PackBlueprint{
		Size:   c0.Size + c1.Size,
		Chunks: []object.Chunk{c0, c1},
	}
	assert.Equal(t, []PackBlueprint{pb}, res.Blueprints)
}

func randSum() sum.Sum {
	s := sum.Sum{}
	rand.Read(s[:])
	return s
}
