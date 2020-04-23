package db

import (
	"database/sql"
	"fmt"
)

// Empty returns an adapter to a new in-memory database with all tables created.
func Empty() (*Adapter, error) {
	sdb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("connecting to in-memory SQLite instance: %v", err)
	}
	if err := sdb.Ping(); err != nil {
		return nil, fmt.Errorf("pinging in-memory SQLite instance: %v", err)
	}
	dba := NewAdapter(sdb)
	if err := dba.InitSchema(); err != nil {
		return nil, fmt.Errorf("creating db schema: %v", err)
	}
	return dba, nil
}
