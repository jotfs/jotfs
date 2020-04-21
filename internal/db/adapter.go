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
	a.mut.Lock()
	defer a.mut.Unlock()

	tx, err := a.db.Begin()
	if err != nil {
		return err
	}

	// Insert a row for the pack
	q := insertOne("packs", []string{"sum", "num_chunks", "size"})
	res, err := tx.Exec(q, index.Sum, len(index.Blocks), index.Size())
	if err != nil {
		log.OnError(tx.Rollback)
		return err
	}
	packID, err := res.LastInsertId()
	if err != nil {
		log.OnError(tx.Rollback)
		return err
	}

	// Insert a row for each block
	q = insertOne("indexes", []string{"pack", "sum", "chunk_size", "mode", "offset", "size"})
	for _, b := range index.Blocks {
		_, err = tx.Exec(q, packID, b.Sum, b.ChunkSize, b.Mode, b.Offset, b.Size)
		if err != nil {
			log.OnError(tx.Rollback)
			return err
		}
	}

	return tx.Commit()
}

func (a *Adapter) InsertFileWithNewVersion(file object.File) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	tx, err := a.db.Begin()
	if err != nil {
		return err
	}

	// Check if the file has already been inserted, if not, add a row
	q := "SELECT id FROM files WHERE name = ?"
	row := tx.QueryRow(q, file.Name)
	var fileID int64
	var nextVersion int
	if err := row.Scan(&fileID); err == sql.ErrNoRows {
		nextVersion = 0
		fileID, err = insertNewFile(tx, file.Name)
		if err != nil {
			log.OnError(tx.Rollback)
			return err
		}
	} else if err == nil {
		v, err := getLatestVersion(tx, fileID)
		if err != nil {
			log.OnError(tx.Rollback)
			return err
		}
		nextVersion = v + 1
	} else {
		log.OnError(tx.Rollback)
		return err
	}

	// Insert a row for the versioned file
	createdAt := time.Now().Unix()
	q = insertOne("file_versions", []string{"file", "version", "created_at", "size", "num_chunks"})
	res, err := tx.Exec(q, fileID, nextVersion, createdAt, file.Size(), len(file.Chunks))
	if err != nil {
		log.OnError(tx.Rollback)
		return err
	}
	fileVerID, err := res.LastInsertId()
	if err != nil {
		log.OnError(tx.Rollback)
		return err
	}

	// Insert a row for each chunk in the file
	q = insertOne("file_contents", []string{"file_version", "idx", "sequence"})
	for _, c := range file.Chunks {
		idxID, err := getPackIndexID(tx, c.Sum)
		if err == sql.ErrNoRows {
			log.OnError(tx.Rollback)
			return fmt.Errorf("no pack index for chunk %x", c.Sum)
		} else if err != nil {
			log.OnError(tx.Rollback)
			return err
		}
		if _, err = tx.Exec(q, fileVerID, idxID, c.Sequence); err != nil {
			log.OnError(tx.Rollback)
			return err
		}
	}

	return tx.Commit()
}

// getLatestVersion gets the latest version of a file. Returns error sql.ErrNoRows if the
// file doesn't exist.
func getLatestVersion(tx *sql.Tx, fileID int64) (int, error) {
	q := "SELECT max(version) FROM file_versions WHERE file = ?"
	row := tx.QueryRow(q, fileID)
	var version int
	err := row.Scan(&version)
	return version, err
}

// insertNewFile adds a new row to the files table. Returns the row ID.
func insertNewFile(tx *sql.Tx, name string) (int64, error) {
	q := insertOne("files", []string{"name"})
	res, err := tx.Exec(q, name)
	if err != nil {
		return 0, err
	}
	fileID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return fileID, nil
}

// getPackIndexID gets *a* row ID for a pack index corresponding to the chunk.
// Note: a chunk may be found in multiple packfiles. Here we just return the first one
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
