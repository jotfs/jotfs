package object

import (
	"bytes"
	"testing"

	"github.com/jotfs/jotfs/internal/compress"
	"github.com/jotfs/jotfs/internal/sum"
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

func TestFilterPackfile(t *testing.T) {
	buf := new(bytes.Buffer)
	builder, err := NewPackfileBuilder(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Add 3 chunks -- a, b, a
	aSum := sum.Compute(a)
	bSum := sum.Compute(b)
	if err = builder.Append(a, aSum, compress.None); err != nil {
		t.Fatal(err)
	}
	if err = builder.Append(b, bSum, compress.Zstd); err != nil {
		t.Fatal(err)
	}
	if err = builder.Append(a, aSum, compress.Zstd); err != nil {
		t.Fatal(err)
	}
	packfile := make([]byte, buf.Len())
	copy(packfile, buf.Bytes())

	// Remove the second block
	filter := func(i uint64) bool { return i != 1 }
	newBuf := new(bytes.Buffer)
	n, err := FilterPackfile(bytes.NewReader(packfile), newBuf, filter)
	assert.NoError(t, err)

	// Size of new packfile should equal number returned from FilterPackfile
	assert.Equal(t, uint64(newBuf.Len()), n)

	// New packfile should contain only 2 blocks: a and a
	newPackfile := make([]byte, n)
	copy(newPackfile, newBuf.Bytes())
	newIndex, err := LoadPackIndex(bytes.NewReader(newPackfile))
	assert.NoError(t, err)
	assert.Equal(t, n, newIndex.Size)
	assert.Equal(t, sum.Compute(newPackfile), newIndex.Sum)
	assert.Equal(t, []sum.Sum{aSum, aSum}, chunkSums(newIndex.Blocks))
}

func TestFilterPackfileAll(t *testing.T) {
	// Filter all blocks from the packfile
	buf := new(bytes.Buffer)
	builder, err := NewPackfileBuilder(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Add 2 chunks -- a, b
	aSum := sum.Compute(a)
	bSum := sum.Compute(b)
	if err = builder.Append(a, aSum, compress.None); err != nil {
		t.Fatal(err)
	}
	if err = builder.Append(b, bSum, compress.Zstd); err != nil {
		t.Fatal(err)
	}
	packfile := make([]byte, buf.Len())
	copy(packfile, buf.Bytes())

	// Remove all blocks -- the resulting packfile should be empty
	filter := func(i uint64) bool { return false }
	newBuf := new(bytes.Buffer)
	n, err := FilterPackfile(bytes.NewReader(packfile), newBuf, filter)
	assert.NoError(t, err)
	assert.Zero(t, n)
	assert.Zero(t, newBuf.Len())
}

func chunkSums(blocks []BlockInfo) []sum.Sum {
	sums := make([]sum.Sum, len(blocks))
	for i, block := range blocks {
		sums[i] = block.Sum
	}
	return sums
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
