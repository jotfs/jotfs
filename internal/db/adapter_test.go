package db

import (
	"testing"
	"time"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/sum"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var s0, _ = sum.FromHex("115f26607addd2a2b0ffc09348f6c2af367ffa5d3dd4aa663c763b6cb375a4ff")
var s1, _ = sum.FromHex("b54d7ba25139e8177c008630f35fbf5bd159953e48c16bdcf400bd3f83bb06e6")

var block0 = object.BlockInfo{
	Sum:       s0,
	ChunkSize: 1505148,
	Sequence:  0,
	Offset:    1,
	Size:      1505189,
	Mode:      compress.None,
}

var block1 = object.BlockInfo{
	Sum:       s1,
	ChunkSize: 610082,
	Sequence:  1,
	Offset:    1505190,
	Size:      610123,
	Mode:      compress.None,
}

var s, _ = sum.FromHex("3418bd8fd971059a773ac36af9e4160f7c2aa410ccefdb4fcabe1a2ff27975e7")
var index = object.PackIndex{Sum: s, Blocks: []object.BlockInfo{block0, block1}, Size: 100}

func TestPackIndex(t *testing.T) {
	db, err := EmptyInMemory()
	if err != nil {
		t.Fatal(err)
	}

	// InsertPackIndex test
	createdAt := time.Now().UTC()
	assert.NoError(t, db.InsertPackIndex(index, createdAt))

	// InsertPackIndex empty -- should get error
	err = db.InsertPackIndex(object.PackIndex{}, createdAt)
	assert.Error(t, err)

	// ChunkExist test
	sums := []sum.Sum{block0.Sum, block1.Sum, {}}
	exists, err := db.ChunksExist(sums)
	assert.NoError(t, err)
	assert.Equal(t, []bool{true, true, false}, exists)

	// ChunksExist empty payload
	exists, err = db.ChunksExist(nil)
	assert.NoError(t, err)
	assert.Empty(t, exists)

	// GetChunkSize test
	size, err := db.GetChunkSize(block0.Sum)
	assert.NoError(t, err)
	assert.Equal(t, block0.ChunkSize, size)

	// GetChunkSize not found
	size, err = db.GetChunkSize(sum.Sum{})
	assert.Equal(t, ErrNotFound, err)
	assert.Zero(t, size)

	// InsertFile test
	chunks := []object.Chunk{
		{Sequence: 0, Size: block0.ChunkSize, Sum: block0.Sum},
		{Sequence: 1, Size: block1.ChunkSize, Sum: block1.Sum},
	}
	file := object.File{
		Name:      "test.txt",
		CreatedAt: time.Now(),
		Chunks:    chunks,
		Versioned: true,
	}
	fs0 := sum.Compute([]byte{0})
	err = db.InsertFile(file, fs0)
	assert.NoError(t, err)

	// InsertFile -- error if name is empty
	file = object.File{
		Name:      "",
		CreatedAt: time.Now(),
		Chunks:    chunks,
	}
	fs1 := sum.Compute([]byte{1})
	err = db.InsertFile(file, fs1)
	assert.Error(t, err)

	// InsertFile -- error if time is zero
	file = object.File{
		Name:      "test.txt",
		CreatedAt: time.Time{},
		Chunks:    chunks,
	}
	fs2 := sum.Compute([]byte{2})
	err = db.InsertFile(file, fs2)
	assert.Error(t, err)

	// InsertFile -- no chunks is fine
	file = object.File{
		Name:      "test.txt",
		CreatedAt: time.Now(),
		Chunks:    []object.Chunk{},
	}
	fs3 := sum.Compute([]byte{3})
	err = db.InsertFile(file, fs3)
	assert.NoError(t, err)

	// InsertFile -- error if chunk does not exist
	file = object.File{
		Name:      "test.txt",
		CreatedAt: time.Now(),
		Chunks:    []object.Chunk{{Sequence: 0, Size: 100, Sum: sum.Sum{}}},
	}
	fs4 := sum.Compute([]byte{4})
	err = db.InsertFile(file, fs4)
	err = db.InsertFile(file, sum.Compute([]byte{4}))
	assert.Error(t, err)
}

func insertFile(t *testing.T, db *Adapter, name string) (sum.Sum, object.File) {
	chunks := []object.Chunk{
		{Sequence: 0, Size: block0.ChunkSize, Sum: block0.Sum},
		{Sequence: 1, Size: block1.ChunkSize, Sum: block1.Sum},
	}
	file := object.File{
		Name:      name,
		CreatedAt: time.Now().UTC(),
		Chunks:    chunks,
		Versioned: true,
	}
	s := sum.Compute(file.MarshalBinary())
	if err := db.InsertFile(file, s); err != nil {
		t.Fatal(err)
	}
	return s, file
}

func TestGetFile(t *testing.T) {
	db, err := EmptyInMemory()
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().UTC()
	if err = db.InsertPackIndex(index, createdAt); err != nil {
		t.Fatal(err)
	}

	s1, f1 := insertFile(t, db, "/test1")
	s2, f2 := insertFile(t, db, "/data/test2")
	s3, f3 := insertFile(t, db, "/data/test2")
	info1 := FileInfo{Name: f1.Name, CreatedAt: f1.CreatedAt, Size: f1.Size(), Sum: s1, Versioned: f1.Versioned}
	info2 := FileInfo{Name: f2.Name, CreatedAt: f2.CreatedAt, Size: f2.Size(), Sum: s2, Versioned: f2.Versioned}
	info3 := FileInfo{Name: f3.Name, CreatedAt: f3.CreatedAt, Size: f3.Size(), Sum: s3, Versioned: f3.Versioned}

	// GetFile
	fg1, err := db.GetFile(s1)
	assert.NoError(t, err)
	assert.Equal(t, f1, fg1)

	// GetFile -- should get ErrNotFound if file does not exist
	_, err = db.GetFile(sum.Sum{})
	assert.Equal(t, ErrNotFound, err)

	// GetFileInfo
	infog1, err := db.GetFileInfo(s1)
	assert.NoError(t, err)
	assert.Equal(t, info1, infog1)

	// GetFileInfo -- error if file does not exist
	_, err = db.GetFileInfo(sum.Sum{})
	assert.Equal(t, ErrNotFound, err)

	// ListFiles
	infos, err := db.ListFiles("/", 0, 100, "", "", false)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{info3, info2, info1}, infos)

	// ListFiles -- ascending
	infos, err = db.ListFiles("/", 0, 100, "", "", true)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{info1, info2, info3}, infos)

	// ListFile -- exclude all filter
	infos, err = db.ListFiles("/", 0, 100, "/*", "", false)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{}, infos)

	// ListFile -- exclude + include filter
	infos, err = db.ListFiles("/", 0, 100, "/*", "/test1", false)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{info1}, infos)

	// GetFileVersions
	infos, err = db.GetFileVersions("/data/test2", 0, 100, false)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{info3, info2}, infos)

	// GetFileVersions -- ascending
	infos, err = db.GetFileVersions("/data/test2", 0, 100, true)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{info2, info3}, infos)

	// GetLatestFileVersion
	info, err := db.GetLatestFileVersion("/data/test2")
	assert.NoError(t, err)
	assert.Equal(t, info3, info)

	// GetLatestVersion -- error if file does not exist
	_, err = db.GetLatestFileVersion("/does-not-exist")
	assert.Equal(t, ErrNotFound, err)

	// GetFileChunks
	indexes, err := db.GetFileChunks(s1)
	assert.NoError(t, err)
	assert.NotEmpty(t, indexes)

	// GetFileChunks -- error if file does not exist
	_, err = db.GetFileChunks(sum.Sum{})
	assert.Equal(t, ErrNotFound, err)

	// Delete file
	err = db.DeleteFile(s1)
	assert.NoError(t, err)
	infos, err = db.ListFiles("/", 0, 10, "/", "/", false)
	assert.NoError(t, err)
	assert.Equal(t, []FileInfo{info3, info2}, infos) // row should be deleted

	// Delete file -- error if file does not exist
	err = db.DeleteFile(sum.Sum{})
	assert.Equal(t, ErrNotFound, err)
}

func TestVacuum(t *testing.T) {
	db, err := EmptyInMemory()
	if err != nil {
		t.Fatal(err)
	}

	// Insert vacuum
	startedAt := time.Now()
	id, err := db.InsertVacuum(startedAt)
	assert.NoError(t, err)

	// Get vacuum
	vac, err := db.GetVacuum(id)
	assert.Equal(t, startedAt.UnixNano(), vac.StartedAt)
	assert.Equal(t, VacuumRunning, vac.Status)
	assert.Equal(t, id, vac.ID)
	assert.Zero(t, vac.CompletedAt)

	// Update vacuum status
	completedAt := time.Now().Add(10 * time.Second)
	err = db.UpdateVacuum(id, completedAt, VacuumOK)
	assert.NoError(t, err)

	// CompletedAt should be set after update
	vac2, err := db.GetVacuum(id)
	assert.NoError(t, err)
	assert.Equal(t,
		Vacuum{ID: id, Status: VacuumOK, StartedAt: startedAt.UnixNano(), CompletedAt: completedAt.UnixNano()},
		vac2,
	)

	// Error from GetVacuum if id does not exist
	_, err = db.GetVacuum("")
	assert.Equal(t, ErrNotFound, err)

	// No Error from UpdateVacuum if id does not exist
	err = db.UpdateVacuum("", completedAt, VacuumOK)
	assert.NoError(t, err)
}

func TestServerStats(t *testing.T) {
	db, err := EmptyInMemory()
	if err != nil {
		t.Fatal(err)
	}

	// Stats should all be zero for empty DB
	stats, err := db.GetServerStats()
	assert.NoError(t, err)
	assert.Equal(t, Stats{}, stats)

	// Insert a file and get stats
	assert.NoError(t, db.InsertPackIndex(index, time.Now()))
	insertFile(t, db, "abc")
	stats, err = db.GetServerStats()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.NumFileVersions)
	assert.Equal(t, uint64(1), stats.NumFiles)
	assert.NotZero(t, stats.TotalFilesSize)
	assert.NotZero(t, stats.TotalDataSize)
}

func TestDeletePackIndex(t *testing.T) {
	db, err := EmptyInMemory()
	if err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, db.InsertPackIndex(index, time.Now()))

	// Delete
	err = db.DeletePackIndex(index.Sum)
	assert.NoError(t, err)

	// No error when Delete is called on an index that doesn't exist
	err = db.DeletePackIndex(sum.Sum{})
	assert.NoError(t, err)
}
