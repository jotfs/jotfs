package object

import (
	"bytes"
	"testing"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/sum"
	"github.com/stretchr/testify/assert"
)

func TestPackfileBuilder(t *testing.T) {
	buf := new(bytes.Buffer)
	builder, err := NewPackfileBuilder(buf)
	assert.NoError(t, err)

	aSum := sum.Compute(a)
	err = builder.Append(a, aSum, compress.None)
	assert.NoError(t, err)

	bSum := sum.Compute(b)
	err = builder.Append(b, bSum, compress.Zstd)
	assert.NoError(t, err)

	index := builder.Build()
	packfile := make([]byte, buf.Len())
	copy(packfile, buf.Bytes())

	assert.Len(t, index.Blocks, 2)
	chunks := [][]byte{a, b}
	sums := []sum.Sum{aSum, bSum}
	for i, block := range index.Blocks {
		assert.Equal(t, uint64(len(chunks[i])), block.ChunkSize)
		assert.Equal(t, sums[i], block.Sum)
		assert.Equal(t, uint64(i), block.Sequence)
	}

	// assert.Equal(t, index.Sum.AsHex(), "f316ea532329ffc9fddf616d1791abe6c4b637430400958e0016ca5268f73a87")

	// Reload the index and check it matches the original
	indexLoad, err := LoadPackIndex(bytes.NewReader(packfile))
	assert.NoError(t, err)
	assert.Equal(t, index, indexLoad)

	// Test marshal / unmarshal of index
	b := index.MarshalBinary()
	indexB := &PackIndex{}
	err = indexB.UnmarshalBinary(b)
	assert.NoError(t, err)
	assert.Equal(t, index, *indexB)
}

func TestEmptyBuilder(t *testing.T) {
	buf := new(bytes.Buffer)
	builder, err := NewPackfileBuilder(buf)
	assert.NoError(t, err)

	index := builder.Build()
	assert.Empty(t, index.Blocks)
}

var a = []byte(`A celebrated tenor had sung in Italian, and a notorious contralto had sung 
 in jazz, and between the numbers people were doing "stunts." all over the garden, while 
happy, vacuous bursts of laughter rose toward the summer sky. A pair of stage twins, who 
turned out to be the girls in yellow, did a baby act in costume, and champagne was served 
in glasses bigger than finger-bowls. The moon had risen higher, and floating in the Sound 
was a triangle of silver scales, trembling a little to the stiff, tinny drip of the 
banjoes on the lawn.`)

var b = []byte(`And as I sat there brooding on the old, unknown world, I thought of Gatsby’s 
wonder when he first picked out the green light at the end of Daisy’s dock. He had come 
a long way to this blue lawn, and his dream must have seemed so close that he could 
hardly fail to grasp it. He did not know that it was already behind him, somewhere back 
in that vast obscurity beyond the city, where the dark fields of the republic rolled on 
under the night. Gatsby believed in the green light, the orgastic future that year by 
year recedes before us. It eluded us then, but that’s no matter -- tomorrow we will run 
faster, stretch out our arms farther... And one fine morning -- So we beat on, boats 
against the current, borne back ceaselessly into the past.`)
