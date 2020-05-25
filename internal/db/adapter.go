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
)

// Adapter interfaces with the database.
type Adapter struct {
	mut sync.Mutex
	db  *sql.DB
}

func NewAdapter(db *sql.DB) *Adapter {
	return &Adapter{sync.Mutex{}, db}
}

func (a *Adapter) InitSchema() error {
	_, err := a.db.Exec(Q_000_Base)
	return err
}

func (a *Adapter) Close() error {
	return a.db.Close()
}

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

func (a *Adapter) ChunkExists(s sum.Sum) (bool, error) {
	// TODO: implement
	return false, nil
}

// GetFileInfo returns the FileInfo for a given file version. Returns ErrNotFound if the
// file does not exist.
func (a *Adapter) GetFileInfo(s sum.Sum) (FileInfo, error) {
	q := `
	SELECT name, created_at, size
	FROM file_versions JOIN files on files.id = file_versions.file 
	WHERE sum = ?
	`
	row := a.db.QueryRow(q, s[:])
	var name string
	var createdAt int64
	var size uint64
	if err := row.Scan(&name, &createdAt, &size); err == sql.ErrNoRows {
		return FileInfo{}, ErrNotFound
	} else if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{Name: name, CreatedAt: time.Unix(0, createdAt), Size: size, Sum: s}, nil
}

// ChunksExist checks if chunks, identified by their checksum, exist in the file store.
// Returns a bool for each chunk.
func (a *Adapter) ChunksExist(sums []sum.Sum) ([]bool, error) {
	if len(sums) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(
		"SELECT DISTINCT sum FROM indexes WHERE sum IN (%s)",
		strings.Repeat("?, ", len(sums)-1)+"?",
	)
	args := make([]interface{}, len(sums))
	for i, s := range sums {
		args[i] = s[:]
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
func (a *Adapter) InsertPackIndex(index object.PackIndex, id string) error {
	return a.update(func(tx *sql.Tx) error {
		packID, err := insertPackfile(tx, index, id)
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

// func incrementRefcount(tx *sql.Tx, )

func (a *Adapter) GetFile(s sum.Sum) (object.File, error) {
	q := "SELECT id, file, created_at, num_chunks FROM file_versions WHERE sum = ?"
	row := a.db.QueryRow(q, s[:])
	var versionID int64
	var fileID int64
	var createdAt int64
	var numChunks uint64
	err := row.Scan(&versionID, &fileID, &createdAt, &numChunks)
	if err == sql.ErrNoRows {
		return object.File{}, ErrNotFound
	}
	if err != nil {
		return object.File{}, err
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

	return object.File{Name: name, CreatedAt: time.Unix(0, createdAt), Chunks: chunks}, nil
}

func (a *Adapter) ListFiles(prefix string, offset int64, limit uint64, exclude string, include string) ([]FileInfo, error) {
	q := `
	SELECT name, created_at, size, sum 
	FROM files JOIN file_versions ON files.id = file_versions.file
	WHERE name LIKE ? AND created_at > ? %s
	ORDER BY created_at DESC
	LIMIT ?
	`
	var rows *sql.Rows
	var err error
	if exclude != "" && include != "" {
		q = fmt.Sprintf(q, "AND ((NOT (name GLOB ?)) OR name GLOB ?)")
		rows, err = a.db.Query(q, prefix+"%", offset, exclude, include, limit)
	} else if exclude != "" {
		q = fmt.Sprintf(q, "AND NOT name GLOB ?")
		rows, err = a.db.Query(q, prefix+"%", offset, exclude, limit)
	} else {
		q = fmt.Sprintf(q, "")
		rows, err = a.db.Query(q, prefix+"%", offset, limit)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var name string
	var createdAt int64
	var size uint64
	s := make([]byte, sum.Size)
	infos := make([]FileInfo, 0)
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(&name, &createdAt, &size, &s); err != nil {
			return nil, err
		}
		sum, err := sum.FromBytes(s)
		if err != nil {
			return nil, err
		}
		info := FileInfo{Name: name, CreatedAt: time.Unix(0, createdAt), Size: size, Sum: sum}
		infos = append(infos, info)
	}

	return infos, nil
}

func (a *Adapter) GetFileVersions(name string, offset int64, limit uint64) ([]FileInfo, error) {
	q := `
	SELECT created_at, size, sum 
	FROM files JOIN file_versions ON files.id = file_versions.file
	WHERE name = ? AND created_at > ?
	ORDER BY created_at DESC
	LIMIT ?
	`
	rows, err := a.db.Query(q, name, offset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var createdAt int64
	var size uint64
	s := make([]byte, sum.Size)
	infos := make([]FileInfo, 0)
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(&createdAt, &size, &s); err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		sum, err := sum.FromBytes(s)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		info := FileInfo{Name: name, CreatedAt: time.Unix(0, createdAt), Size: size, Sum: sum}
		infos = append(infos, info)
	}

	return infos, nil
}

type FileInfo struct {
	Name      string
	CreatedAt time.Time
	Size      uint64
	Sum       sum.Sum
}

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

func insertPackfile(tx *sql.Tx, index object.PackIndex, id string) (int64, error) {
	q := insertOne("packs", []string{"sum", "num_chunks", "size", "file_id"})
	res, err := tx.Exec(q, index.Sum[:], len(index.Blocks), index.Size(), id)
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
	q := insertOne("file_versions", []string{"file", "created_at", "size", "num_chunks", "sum"})
	res, err := tx.Exec(q, fileID, file.CreatedAt.UnixNano(), file.Size(), len(file.Chunks), sum[:])
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

		// Decrement the refcount of each chunk referenced by this file by one
		q := `
		UPDATE indexes 
		SET refcount = refcount - 1 
		WHERE id IN (SELECT idx FROM file_contents WHERE file_version = ?)
		`
		if _, err := tx.Exec(q, verID); err != nil {
			return fmt.Errorf("decrementing index refcount: %w", err)
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

func insertOne(table string, cols []string) string {
	v := strings.Repeat("?,", len(cols)-1)
	v = "(" + v + "?)"
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table, strings.Join(cols, ","), v,
	)
}

func insertMany(table string, cols []string, n int) string {
	v := strings.Repeat("?,", len(cols)-1)
	v = "(" + v + "?),"
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s%s",
		table, strings.Join(cols, ","), strings.Repeat(v, n-1), v[:len(v)-1],
	)
}
