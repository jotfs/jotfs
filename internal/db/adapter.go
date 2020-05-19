package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

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

type ChunkIndex struct {
	Sequence uint64
	PackSum  sum.Sum
	Block    object.BlockInfo
}

func (a *Adapter) GetFileChunks(s sum.Sum) ([]ChunkIndex, error) {

	// Get the row ID for the file version and
	q := "SELECT id, num_chunks FROM file_versions WHERE sum = ?"
	var verID int64
	var nChunks int
	row := a.db.QueryRow(q, s)
	if err := row.Scan(&verID, &nChunks); err == sql.ErrNoRows {
		return nil, fmt.Errorf("not found")
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
			indexes.sequence
			packs.sum
		FROM 
			file_contents 
			JOIN indexes ON indexes.id = file_contents.idx
			JOIN packs ON packs.id = indexes.pack
		WHERE
			file_contents.file_version = ?
		ORDER BY
			file_contents.sequence
	`
	rows, err := a.db.Query(q, verID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunks := make([]ChunkIndex, nChunks)
	var i int
	for ; rows.Next(); i++ {
		if i >= nChunks {
			return nil, fmt.Errorf("number of chunks greater than expected %d", nChunks)
		}
		var (
			cSeq    uint64
			cSum    sum.Sum
			cSize   uint64
			mode    uint8
			bOffset uint64
			bSize   uint64
			bSeq    uint64
			pSum    sum.Sum
		)
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
		chunks[i] = ChunkIndex{
			Sequence: cSeq,
			PackSum:  pSum,
			Block: object.BlockInfo{
				Sum:       cSum,
				ChunkSize: cSize,
				Sequence:  bSeq,
				Offset:    bOffset,
				Size:      bSize,
				Mode:      cmode,
			},
		}
	}
	if i != nChunks-1 {
		return nil, fmt.Errorf("expected %d chunks but received %d", nChunks, i)
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
		[]string{"pack", "sequence", "sum", "chunk_size", "mode", "offset", "size"},
	)
	for _, b := range blocks {
		_, err := tx.Exec(q, packID, b.Sequence, b.Sum[:], b.ChunkSize, b.Mode, b.Offset, b.Size)
		if err != nil {
			return err
		}
	}
	return nil
}

// func getPackBlocks(tx *sql.Tx, packID int64) ([]object.BlockInfo, error) {
// 	q := `
// 		SELECT pack, sequence, sum, chunk_size, mode, offset, size
// 		FROM indexes
// 		WHERE pack = $1
// 	`
// 	rows, err := tx.Query(q, packID)
// 	if err != nil {
// 		return nil, err
// 	}
// 	blocks := make([]object.BlockInfo, 0)
// 	for rows.Next() {

// 	}

// }

func insertFileChunks(tx *sql.Tx, fileVerID int64, chunks []object.Chunk) error {
	q := insertOne("file_contents", []string{"file_version", "idx", "sequence"})
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
	}
	return nil
}

func insertFileVersion(tx *sql.Tx, fileID int64, file object.File, sum sum.Sum) (int64, error) {
	q := insertOne("file_versions", []string{"file", "created_at", "size", "num_chunks", "sum"})
	res, err := tx.Exec(q, fileID, file.CreatedAt, file.Size(), len(file.Chunks), sum[:])
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
