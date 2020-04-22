package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/iotafs/iotafs/internal/log"
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
		log.OnError(tx.Rollback)
		return err
	}
	return tx.Commit()
}

// ChunksExist checks if chunks, identified by their checksum, exist in the file store.
// Returns a bool for each chunk.
func (a *Adapter) ChunksExist(sums []sum.Sum) ([]bool, error) {
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
	for rows.Next() {
		var s sum.Sum
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
	}

	result := make([]bool, len(sums))
	for i, s := range sums {
		_, ok := exists[s]
		result[i] = ok
	}

	return result, nil
}

// InsertPackIndex saves a PackIndex to the database.
func (a *Adapter) InsertPackIndex(index object.PackIndex) error {
	return a.update(func(tx *sql.Tx) error {
		packID, err := insertPackfile(tx, index)
		if err != nil {
			return err
		}
		return insertPackBlocks(tx, packID, index.Blocks)
	})
}

// InsertFile saves a File object to the database.
func (a *Adapter) InsertFile(file object.File, sum sum.Sum) error {
	return a.update(func(tx *sql.Tx) error {
		fileID, err := insertFileIfNotExists(tx, file.Name)
		if err != nil {
			return err
		}
		fileVerID, err := insertFileVersion(tx, fileID, file, sum)
		if err != nil {
			return err
		}
		return insertFileChunks(tx, fileVerID, file.Chunks)
	})
}

func insertPackfile(tx *sql.Tx, index object.PackIndex) (int64, error) {
	q := insertOne("packs", []string{"sum", "num_chunks", "size"})
	res, err := tx.Exec(q, index.Sum, len(index.Blocks), index.Size())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertPackBlocks(tx *sql.Tx, packID int64, blocks []object.BlockInfo) error {
	q := insertOne("indexes", []string{"pack", "sum", "chunk_size", "mode", "offset", "size"})
	for _, b := range blocks {
		_, err := tx.Exec(q, packID, b.Sum, b.ChunkSize, b.Mode, b.Offset, b.Size)
		if err != nil {
			return err
		}
	}
	return nil
}

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
	createdAt := time.Now().UnixNano()
	q := insertOne("file_versions", []string{"file", "created_at", "size", "num_chunks", "sum"})
	res, err := tx.Exec(q, fileID, createdAt, file.Size(), len(file.Chunks), sum[:])
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
	row := tx.QueryRow(q, sum)
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
