package object

import (
	"bytes"
	"testing"
	"time"

	"github.com/iotafs/iotafs/internal/sum"
	"github.com/stretchr/testify/assert"
)

func TestFileMarshalUnmarshal(t *testing.T) {
	c0 := Chunk{Sequence: 0, Size: 100, Sum: sum.Compute([]byte("a"))}
	c1 := Chunk{Sequence: 1, Size: 100, Sum: sum.Compute([]byte("b"))}

	tests := []File{
		{"abc", time.Now().UTC(), []Chunk{c0, c1}, true},
		{"abc", time.Now().UTC(), []Chunk{c0, c1}, false},
		{"abc", time.Now().UTC(), []Chunk{}, false},
		{"", time.Now().UTC(), []Chunk{c0, c0, c1}, true},
	}

	for i, file := range tests {
		b := file.MarshalBinary()
		bfile := new(File)
		err := bfile.UnmarshalBinary(bytes.NewReader(b))
		assert.NoError(t, err, i)
		assert.Equal(t, file, *bfile, i)
	}

	assert.Equal(t, uint64(200), tests[0].Size())
	assert.Equal(t, uint64(0), tests[2].Size())

}
