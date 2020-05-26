package db

import (
	"log"
	"testing"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/sum"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestPackIndex(t *testing.T) {
	db, err := EmptyInMemory()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	id, index := packIndexExample()
	assert.NoError(t, db.InsertPackIndex(index, id))
}

func packIndexExample() (string, object.PackIndex) {
	return "id", object.PackIndex{
		Sum: sum.Compute([]byte("123456789")),
		Blocks: []object.BlockInfo{
			{
				Sum:       sum.Compute([]byte("a")),
				ChunkSize: 100,
				Sequence:  0,
				Offset:    0,
				Size:      100,
				Mode:      compress.None,
			},
		},
	}
}
