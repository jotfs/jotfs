package testdb

import (
	"database/sql"
	"fmt"

	"github.com/iotafs/iotafs/internal/db"
)

// Empty returns an adapter to a new in-memory database with all tables created.
func Empty() (*db.Adapter, error) {
	sdb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("connecting to in-memory SQLite instance: %v", err)
	}
	if err := sdb.Ping(); err != nil {
		return nil, fmt.Errorf("pinging in-memory SQLite instance: %v", err)
	}
	dba := db.NewAdapter(sdb)
	if err := dba.InitSchema(); err != nil {
		return nil, fmt.Errorf("creating db schema: %v", err)
	}
	return dba, nil
}
