package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/iotafs/iotafs/internal/compress"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/sum"
	"github.com/rs/xid"
)

// VacuumStatus represents the status of a vacuum process.
type VacuumStatus int

// Vacuum status codes
const (
	VacuumRunning VacuumStatus = iota
	VacuumOK
	VacuumFailed
)

func (s VacuumStatus) String() string {
	switch s {
	case VacuumRunning:
		return "RUNNING"
	case VacuumOK:
		return "SUCCEEDED"
	case VacuumFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Adapter interfaces with the database.
type Adapter struct {
	mut sync.Mutex
	db  *sql.DB
}

// NewAdapter returns a new database adapter.
func NewAdapter(db *sql.DB) *Adapter {
	return &Adapter{sync.Mutex{}, db}
}

// InitSchema creates the tables for a new database.
func (a *Adapter) InitSchema() error {
	_, err := a.db.Exec(Q_000_Base)
	return err
}

// update accepts a function which may modify the database in a transaction. It cancels
// the transaction if the function returns an error, or commits the transaction otherwise.
func (a *Adapter) update(f func(tx *sql.Tx) error) error {
	a.mut.Lock()
	defer a.mut.Unlock()
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = fmt.Errorf("%w; %v", err, rerr)
		}
		return err
	}
	return tx.Commit()
}

// GetFileInfo returns the FileInfo for a given file version. Returns ErrNotFound if the
// file does not exist.
func (a *Adapter) GetFileInfo(s sum.Sum) (FileInfo, error) {
	q := `
	SELECT name, created_at, size, versioned
	FROM file_versions JOIN files on files.id = file_versions.file 
	WHERE sum = ?
	`
	row := a.db.QueryRow(q, s[:])
	var name string
	var createdAt int64
	var size uint64
	var vflag int
	if err := row.Scan(&name, &createdAt, &size, &vflag); err == sql.ErrNoRows {
		return FileInfo{}, ErrNotFound
	} else if err != nil {
		return FileInfo{}, err
	}

	versioned, err := parseVFlag(vflag)
	if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{
		Name:      name,
		CreatedAt: time.Unix(0, createdAt).UTC(),
		Size:      size,
		Sum:       s,
		Versioned: versioned,
	}, nil
}

func parseVFlag(vflag int) (bool, error) {
	if vflag == 1 {
		return true, nil
	}
	if vflag == 0 {
		return false, nil
	}
	return false, fmt.Errorf("invalid version flag %d", vflag)

}

// ChunksExist checks if chunks, identified by their checksum, exist in the file store.
// Returns a bool for each chunk.
func (a *Adapter) ChunksExist(sums []sum.Sum) ([]bool, error) {
	if len(sums) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(
		"SELECT DISTINCT sum FROM indexes WHERE sum IN (%s) AND delete_marker <> 1",
		strings.Repeat("?, ", len(sums)-1)+"?",
	)
	args := make([]interface{}, len(sums))
	for i := range sums {
		args[i] = sums[i][:]
	}
	rows, err := a.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	exists := make(map[sum.Sum]bool, len(sums))
	b := make([]byte, sum.Size)
	for rows.Next() {
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		s, err := sum.FromBytes(b)
		if err != nil {
			return nil, err
		}
		exists[s] = true
	}

	result := make([]bool, len(sums))
	for i, s := range sums {
		_, ok := exists[s]
		result[i] = ok
	}

	return result, nil
}

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// GetChunkSize gets the size of a chunk. Returns ErrNotFound if the chunk does not exist.
func (a *Adapter) GetChunkSize(s sum.Sum) (uint64, error) {
	q := "SELECT chunk_size FROM indexes WHERE sum = ?"
	row := a.db.QueryRow(q, s[:])
	var size uint64
	if err := row.Scan(&size); err == sql.ErrNoRows {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, err
	}
	return size, nil
}

// InsertPackIndex saves a PackIndex to the database.
func (a *Adapter) InsertPackIndex(index object.PackIndex, createdAt time.Time) error {
	if len(index.Blocks) == 0 {
		return fmt.Errorf("pack index is empty")
	}
	return a.update(func(tx *sql.Tx) error {
		packID, err := insertPackfile(tx, index, createdAt)
		if err != nil {
			return fmt.Errorf("inserting packfile: %w", err)
		}
		err = insertPackBlocks(tx, packID, index.Blocks)
		if err != nil {
			return fmt.Errorf("insert pack blocks: %w", err)
		}
		return nil
	})
}

// InsertFile saves a File object to the database.
func (a *Adapter) InsertFile(file object.File, sum sum.Sum) error {
	return a.update(func(tx *sql.Tx) error {
		fileID, err := insertFileIfNotExists(tx, file.Name)
		if err != nil {
			return fmt.Errorf("inserting file: %w", err)
		}
		fileVerID, err := insertFileVersion(tx, fileID, file, sum)
		if err != nil {
			return fmt.Errorf("inserting file version: %w", err)
		}
		err = insertFileChunks(tx, fileVerID, file.Chunks)
		if err != nil {
			return fmt.Errorf("inserting file chunks: %w", err)
		}
		return nil
	})
}

// GetFile returns a File from the database with a given sum. Returns db.ErrNotFound if
// the file does not exist.
func (a *Adapter) GetFile(s sum.Sum) (object.File, error) {
	q := "SELECT id, file, created_at, num_chunks, versioned FROM file_versions WHERE sum = ?"
	row := a.db.QueryRow(q, s[:])
	var versionID int64
	var fileID int64
	var createdAt int64
	var numChunks uint64
	var vflag int
	err := row.Scan(&versionID, &fileID, &createdAt, &numChunks, &vflag)
	if err == sql.ErrNoRows {
		return object.File{}, ErrNotFound
	}
	if err != nil {
		return object.File{}, err
	}
	versioned, err := parseVFlag(vflag)
	if err != nil {
		return object.File{}, fmt.Errorf("invalid version flag: %d", vflag)
	}

	q = "SELECT name FROM files WHERE id = ?"
	row = a.db.QueryRow(q, fileID)
	var name string
	if err := row.Scan(&name); err != nil {
		return object.File{}, err
	}

	q = `
	SELECT file_contents.sequence, chunk_size, sum
	FROM file_contents JOIN indexes on indexes.id = file_contents.idx
	WHERE file_contents.file_version = ?
	`
	rows, err := a.db.Query(q, versionID)
	if err != nil {
		return object.File{}, err
	}
	defer rows.Close()
	chunks := make([]object.Chunk, 0, numChunks)
	var seq uint64
	var size uint64
	csum := make([]byte, sum.Size)
	for rows.Next() {
		if err := rows.Scan(&seq, &size, &csum); err != nil {
			return object.File{}, err
		}
		sum, err := sum.FromBytes(csum)
		if err != nil {
			return object.File{}, err
		}
		chunks = append(chunks, object.Chunk{Sequence: seq, Size: size, Sum: sum})
	}

	return object.File{
		Name:      name,
		CreatedAt: time.Unix(0, createdAt).UTC(),
		Chunks:    chunks,
		Versioned: versioned,
	}, nil
}

// ListFiles returns a FileInfo slice containing corresponding to files that match the
// provided prefix. Glob parametrs exclude and include are used to filter the result.
// Pagination is acheived using the offset and limit parameters. Results are returned
// in reverse-chronological order by default. Setting ascending to true reverses the
// order.
func (a *Adapter) ListFiles(prefix string, offset int64, limit uint64, exclude string, include string, ascending bool) ([]FileInfo, error) {
	q := `
	SELECT name, created_at, size, sum, versioned 
	FROM files JOIN file_versions ON files.id = file_versions.file
	WHERE name LIKE ? AND created_at > ? %s
	ORDER BY created_at %s
	LIMIT ?
	`
	ord := "DESC"
	if ascending {
		ord = "ASC"
	}

	var rows *sql.Rows
	var err error
	if exclude != "" && include != "" {
		q = fmt.Sprintf(q, "AND ((NOT (name GLOB ?)) OR name GLOB ?)", ord)
		rows, err = a.db.Query(q, prefix+"%", offset, exclude, include, limit)
	} else if exclude != "" {
		q = fmt.Sprintf(q, "AND NOT name GLOB ?", ord)
		rows, err = a.db.Query(q, prefix+"%", offset, exclude, limit)
	} else {
		q = fmt.Sprintf(q, "", ord)
		rows, err = a.db.Query(q, prefix+"%", offset, limit)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var name string
	var createdAt int64
	var size uint64
	var vflag int
	s := make([]byte, sum.Size)
	infos := make([]FileInfo, 0)
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(&name, &createdAt, &size, &s, &vflag); err != nil {
			return nil, err
		}
		sum, err := sum.FromBytes(s)
		if err != nil {
			return nil, err
		}
		versioned, err := parseVFlag(vflag)
		if err != nil {
			return nil, err
		}

		info := FileInfo{
			Name:      name,
			CreatedAt: time.Unix(0, createdAt).UTC(),
			Size:      size,
			Sum:       sum,
			Versioned: versioned,
		}
		infos = append(infos, info)
	}

	return infos, nil
}

// GetLatestFileVersion returns the latest version of a file with a given name. Returns
// db.ErrNotFound if the file does not exist.
func (a *Adapter) GetLatestFileVersion(name string) (FileInfo, error) {
	versions, err := a.GetFileVersions(name, 0, 1, false)
	if err != nil {
		return FileInfo{}, err
	}
	if len(versions) == 0 {
		return FileInfo{}, ErrNotFound
	}
	return versions[0], nil
}

// GetFileVersions returns the versions of a file with a given name. Pagination is
// acheived with the offset and limit parameters.
func (a *Adapter) GetFileVersions(name string, offset int64, limit uint64, ascending bool) ([]FileInfo, error) {
	q := `
	SELECT created_at, size, sum, versioned 
	FROM files JOIN file_versions ON files.id = file_versions.file
	WHERE name = ? AND created_at > ?
	ORDER BY created_at %s
	LIMIT ?
	`
	ord := "DESC"
	if ascending {
		ord = "ASC"
	}
	q = fmt.Sprintf(q, ord)

	rows, err := a.db.Query(q, name, offset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var createdAt int64
	var size uint64
	s := make([]byte, sum.Size)
	var vflag int
	infos := make([]FileInfo, 0)
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(&createdAt, &size, &s, &vflag); err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		sum, err := sum.FromBytes(s)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		versioned, err := parseVFlag(vflag)
		if err != nil {
			return nil, err
		}

		info := FileInfo{
			Name:      name,
			CreatedAt: time.Unix(0, createdAt).UTC(),
			Size:      size,
			Sum:       sum,
			Versioned: versioned,
		}
		infos = append(infos, info)
	}

	return infos, nil
}

// FileInfo stores the metadata associated with a file.
type FileInfo struct {
	Name      string
	CreatedAt time.Time
	Size      uint64
	Sum       sum.Sum
	Versioned bool
}

// ChunkIndex is returned by GetFileChunks.
type ChunkIndex struct {
	Sequence uint64
	PackSum  sum.Sum
	Block    object.BlockInfo
}

// GetFileChunks returns the packfile location of each chunk in a file. Returns
// ErrNotFound if the file does not exist.
func (a *Adapter) GetFileChunks(fileID sum.Sum) ([]ChunkIndex, error) {

	// Get the row id for the file version
	q := "SELECT id, num_chunks FROM file_versions WHERE sum = ?"
	var verID int64
	var nChunks int
	row := a.db.QueryRow(q, fileID[:])
	if err := row.Scan(&verID, &nChunks); err == sql.ErrNoRows {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	q = `
		SELECT 
			file_contents.sequence, 
			indexes.sum,
			indexes.chunk_size,
			indexes.mode,
			indexes.offset,
			indexes.size,
			indexes.sequence,
			packs.sum
		FROM 
			file_contents 
			JOIN indexes ON indexes.id = file_contents.idx
			JOIN packs ON packs.id = indexes.pack
		WHERE file_contents.file_version = ?
		ORDER BY file_contents.sequence
	`
	rows, err := a.db.Query(q, verID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunks := make([]ChunkIndex, nChunks)
	var (
		cSeq    uint64
		cSum    []byte
		cSize   uint64
		mode    uint8
		bOffset uint64
		bSize   uint64
		bSeq    uint64
		pSum    []byte
	)
	var i int
	for ; rows.Next(); i++ {
		if i >= nChunks {
			return nil, fmt.Errorf("number of chunks greater than expected %d", nChunks)
		}

		if err := rows.Scan(&cSeq, &cSum, &cSize, &mode, &bOffset, &bSize, &bSeq, &pSum); err != nil {
			return nil, err
		}
		cmode, err := compress.FromUint8(mode)
		if err != nil {
			return nil, err
		}
		if uint64(i) != cSeq {
			return nil, fmt.Errorf("expected next chunk sequence %d but received %d", i, cSeq)
		}

		cs, err := sum.FromBytes(cSum)
		if err != nil {
			return nil, err
		}
		ps, err := sum.FromBytes(pSum)
		if err != nil {
			return nil, err
		}
		chunks[i] = ChunkIndex{
			Sequence: cSeq,
			PackSum:  ps,
			Block: object.BlockInfo{
				Sum:       cs,
				ChunkSize: cSize,
				Sequence:  bSeq,
				Offset:    bOffset,
				Size:      bSize,
				Mode:      cmode,
			},
		}
	}

	return chunks, nil
}

func insertPackfile(tx *sql.Tx, index object.PackIndex, createdAt time.Time) (int64, error) {
	q := insertOne("packs", []string{"sum", "num_chunks", "size", "created_at"})
	res, err := tx.Exec(q, index.Sum[:], len(index.Blocks), index.Size, createdAt.UnixNano())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertPackBlocks(tx *sql.Tx, packID int64, blocks []object.BlockInfo) error {
	q := insertOne(
		"indexes",
		[]string{"pack", "sequence", "sum", "chunk_size", "mode", "offset", "size", "refcount"},
	)
	for _, b := range blocks {
		_, err := tx.Exec(q, packID, b.Sequence, b.Sum[:], b.ChunkSize, b.Mode, b.Offset, b.Size, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func insertFileChunks(tx *sql.Tx, fileVerID int64, chunks []object.Chunk) error {
	q := insertOne("file_contents", []string{"file_version", "idx", "sequence"})
	qIncRC := "UPDATE indexes SET refcount = refcount + 1 WHERE id = ?"
	for _, c := range chunks {
		idxID, err := getPackIndexID(tx, c.Sum)
		if err == sql.ErrNoRows {
			return fmt.Errorf("no pack index for chunk %x", c.Sum)
		} else if err != nil {
			return err
		}
		if _, err = tx.Exec(q, fileVerID, idxID, c.Sequence); err != nil {
			return err
		}

		// increment the refence count of the chunk index
		if _, err = tx.Exec(qIncRC, idxID); err != nil {
			return fmt.Errorf("incrementing index refcount: %w", err)
		}
	}
	return nil
}

func insertFileVersion(tx *sql.Tx, fileID int64, file object.File, sum sum.Sum) (int64, error) {
	q := insertOne("file_versions", []string{"file", "created_at", "size", "num_chunks", "sum", "versioned"})
	var vflag int
	if file.Versioned {
		vflag = 1
	}
	res, err := tx.Exec(q, fileID, file.CreatedAt.UnixNano(), file.Size(), len(file.Chunks), sum[:], vflag)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertFileIfNotExists(tx *sql.Tx, name string) (int64, error) {
	q := "SELECT id FROM files WHERE name = ?"
	row := tx.QueryRow(q, name)
	var id int64
	if err := row.Scan(&id); err == sql.ErrNoRows {
		q = insertOne("files", []string{"name"})
		res, err := tx.Exec(q, name)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	} else if err != nil {
		return 0, err
	}
	return id, nil
}

// getPackIndexID gets a row ID for a pack index corresponding to a chunk.
// Note: a chunk may be found in multiple packfiles, but we just return the first one
// found.
func getPackIndexID(tx *sql.Tx, sum sum.Sum) (int64, error) {
	q := "SELECT id FROM indexes WHERE sum = ? ORDER BY id"
	row := tx.QueryRow(q, sum[:])
	var id int64
	err := row.Scan(&id)
	return id, err
}

// DeleteFile deletes a file and decrements all chunks referenced by the file by one.
// Returns ErrNotFound if the file does not exist.
func (a *Adapter) DeleteFile(s sum.Sum) error {
	return a.update(func(tx *sql.Tx) error {
		// Get the row ID of the file version
		var verID int64
		var fileID int64
		row := tx.QueryRow("SELECT id, file FROM file_versions WHERE sum = ?", s[:])
		if err := row.Scan(&verID, &fileID); err == sql.ErrNoRows {
			return ErrNotFound
		} else if err != nil {
			return err
		}

		q := "SELECT idx, count(*) FROM file_contents WHERE file_version = ? GROUP BY idx"
		rows, err := tx.Query(q, verID)
		if err != nil {
			return err
		}
		defer rows.Close()
		updateQ := "UPDATE indexes SET refcount = refcount - ? WHERE id = ?"
		var indexID int64
		var rc int64
		for rows.Next() {
			if err := rows.Scan(&indexID, &rc); err != nil {
				return err
			}
			if _, err := tx.Exec(updateQ, rc, indexID); err != nil {
				return err
			}
		}

		// Delete row from file_version and corresponding rows from file_contents
		q = "DELETE FROM file_contents WHERE file_version = ?"
		if _, err := tx.Exec(q, verID); err != nil {
			return fmt.Errorf("deleting file_contents: %w", err)
		}
		q = "DELETE FROM file_versions WHERE id = ?"
		if _, err := tx.Exec(q, verID); err != nil {
			return fmt.Errorf("deleting file_versions: %w", err)
		}

		// Get the number of versions with the same name. If this version is the last,
		// we can delete the row from the files table
		q = "SELECT count(*) FROM file_versions WHERE file = ?"
		row = tx.QueryRow(q, fileID)
		var numVersions int64
		if err := row.Scan(&numVersions); err != nil {
			return err
		}
		if numVersions == 0 {
			q = "DELETE FROM files WHERE id = ?"
			if _, err := tx.Exec(q, fileID); err != nil {
				return fmt.Errorf("deleting file: %w", err)
			}
		}

		return nil
	})
}

// ZeroRefcount is returned by GetZeroRefcount. It stores a pack ID and a sorted sequence
// of each block in the file with a zero refcount.
type ZeroRefcount struct {
	PackID    sum.Sum
	Sequences []uint64
}

// GetZeroRefcount returns the block sequence numbers in each packfile with a zero
// refcount.
func (a *Adapter) GetZeroRefcount(createdBefore time.Time) ([]ZeroRefcount, error) {
	var result []ZeroRefcount

	a.update(func(tx *sql.Tx) error {
		q := `
		SELECT indexes.id, packs.sum, indexes.sequence 
		FROM indexes JOIN packs on packs.id = indexes.pack
		WHERE indexes.refcount = 0 AND packs.created_at < ?
		ORDER BY packs.id, indexes.sequence
		`
		rows, err := a.db.Query(q, createdBefore.UTC().UnixNano())
		if err != nil {
			return err
		}
		defer rows.Close()

		var indexIDs []int64
		var prevSum sum.Sum
		var slice []uint64
		var indexID int64
		var seq uint64
		packID := make([]byte, sum.Size)
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&indexID, &packID, &seq); err != nil {
				return err
			}
			sum, err := sum.FromBytes(packID)
			if err != nil {
				return err
			}
			if prevSum != sum {
				if i != 0 {
					seqs := make([]uint64, len(slice))
					copy(seqs, slice)
					result = append(result, ZeroRefcount{prevSum, seqs})
					slice = slice[:0]
				}
				prevSum = sum
			}
			slice = append(slice, seq)
			indexIDs = append(indexIDs, indexID)
		}
		if len(slice) > 0 { // Don't forget the last slice
			seqs := make([]uint64, len(slice))
			copy(seqs, slice)
			result = append(result, ZeroRefcount{prevSum, seqs})

		}

		// Set the delete marker on all blocks picked out
		q = "UPDATE indexes SET delete_marker = 1 WHERE id = ?"
		for _, id := range indexIDs {
			if _, err := tx.Exec(q, id); err != nil {
				return err
			}
		}

		return nil
	})

	return result, nil
}

// UpdateIndex overwrites the contents of a pack index with a new one. The map m
// specifies the mapping from the sequence numbers of the new index to the sequence
// numbers of the old index. Any sequences in the old index which are not re-mapped will
// be deleted when DeletePackIndex is called on the old index.
func (a *Adapter) UpdateIndex(newIndex object.PackIndex, createdAt time.Time, oldIndexSum sum.Sum, m map[uint64]uint64) error {
	return a.update(func(tx *sql.Tx) error {
		newPackID, err := insertPackfile(tx, newIndex, createdAt.UTC())
		if err != nil {
			return fmt.Errorf("insertPackfile: %w", err)
		}

		var oldPackID uint64
		q := "SELECT id FROM packs WHERE sum = ?"
		row := tx.QueryRow(q, oldIndexSum[:])
		if err := row.Scan(&oldPackID); err != nil {
			return fmt.Errorf("getting old pack row ID: %w", err)
		}

		q = `
		UPDATE indexes 
		SET pack = ?, sequence = ?, offset = ? 
		WHERE pack = ? AND sequence = ?
		`
		for _, block := range newIndex.Blocks {
			oldSeq, ok := m[block.Sequence]
			if !ok {
				return fmt.Errorf("no sequence mapping for block %d", block.Sequence)
			}
			_, err = tx.Exec(q, newPackID, block.Sequence, block.Offset, oldPackID, oldSeq)
			if err != nil {
				return fmt.Errorf("updating pack index: %w", err)
			}
		}

		return nil
	})
}

// DeletePackIndex deletes a pack index from the database.
func (a *Adapter) DeletePackIndex(sum sum.Sum) error {
	return a.update(func(tx *sql.Tx) error {
		// Will also delete corresponding rows in indexes table because the FK has
		// ON DELETE CASCADE set.
		q := "DELETE FROM packs WHERE sum = ?"
		_, err := tx.Exec(q, sum[:])
		return err
	})
}

// InsertVacuum inserts a row for a new vacuum. Returns the vacuum ID.
func (a *Adapter) InsertVacuum(startedAt time.Time) (string, error) {
	var id string
	err := a.update(func(tx *sql.Tx) error {
		id = xid.New().String()
		q := insertOne("vacuums", []string{"id", "started_at", "status"})
		_, err := tx.Exec(q, id, startedAt.UTC().UnixNano(), VacuumRunning)
		return err
	})
	if err != nil {
		return "", err
	}
	return id, err
}

// UpdateVacuum updates the status and completed time of a given vacuum.
func (a *Adapter) UpdateVacuum(id string, completedAt time.Time, status VacuumStatus) error {
	return a.update(func(tx *sql.Tx) error {
		q := "UPDATE vacuums SET completed_at = ?, status = ? WHERE id = ?"
		_, err := tx.Exec(q, completedAt.UTC().UnixNano(), int(status), id)
		return err
	})
}

// Vacuum is returned by the GetVacuum method.
type Vacuum struct {
	ID        string
	Status    VacuumStatus
	StartedAt int64
	// Will be zero if Status is VacuumRunning
	CompletedAt int64
}

// GetVacuum returns the status of a vacuum process with a given ID. Returns
// db.ErrNotFound if the vacuum does not exist.
func (a *Adapter) GetVacuum(id string) (Vacuum, error) {
	q := "SELECT status, started_at, completed_at FROM vacuums WHERE id = ?"
	var status int
	var startedAt int64
	var completedAt int64
	row := a.db.QueryRow(q, id)
	err := row.Scan(&status, &startedAt, &completedAt)
	if err == sql.ErrNoRows {
		return Vacuum{}, ErrNotFound
	}
	if err != nil {
		return Vacuum{}, err
	}
	return Vacuum{id, VacuumStatus(status), startedAt, completedAt}, nil
}

type Stats struct {
	NumFiles        uint64
	NumFileVersions uint64
	TotalFilesSize   uint64
	TotalDataSize   uint64
}

func (a *Adapter) GetServerStats() (Stats, error) {
	var numFiles uint64
	row := a.db.QueryRow("SELECT count(*) FROM files")
	if err := row.Scan(&numFiles); err != nil {
		return Stats{}, err
	}

	var numFileVersions uint64
	row = a.db.QueryRow("SELECT count(*) FROM file_versions")
	if err := row.Scan(&numFileVersions); err != nil {
		return Stats{}, err
	}

	var totalFilesSize uint64
	row = a.db.QueryRow("SELECT sum(size) FROM file_versions")
	if err := row.Scan(&totalFilesSize); err != nil {
		return Stats{}, err
	}

	var totalDataSize uint64
	row = a.db.QueryRow("SELECT sum(size) FROM indexes")
	if err := row.Scan(&totalDataSize); err != nil {
		return Stats{}, err
	}

	return Stats{numFiles, numFileVersions, totalFilesSize, totalDataSize}, nil
}

func insertOne(table string, cols []string) string {
	v := strings.Repeat("?,", len(cols)-1)
	v = "(" + v + "?)"
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table, strings.Join(cols, ","), v,
	)
}
