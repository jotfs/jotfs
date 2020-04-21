package object

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/stretchr/testify/assert"
)

func TestPackfileBuilder(t *testing.T) {
	nChunks := 5

	// Write nChunks random data chunks to the builder
	buf := new(bytes.Buffer)
	builder, err := NewPackfileBuilder(buf)
	assert.NoError(t, err)
	for i := 0; i < nChunks; i++ {
		chunk := randBytes(1024)
		_, err = builder.Append(chunk, compress.None)
		assert.NoError(t, err)
	}
	index := builder.Build()
	assert.Len(t, index.Blocks, nChunks)

	// Reload the index and check it matches the original
	r := bytes.NewBuffer(buf.Bytes())
	indexLoad, err := LoadPackIndex(r)
	assert.NoError(t, err)
	assert.Equal(t, index, indexLoad)

	// Test marshal / unmarshal of index
	b := index.MarshalBinary()
	indexB := &PackIndex{}
	err = indexB.UnmarshalBinary(b)
	assert.NoError(t, err)
	assert.Equal(t, index, *indexB)
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
