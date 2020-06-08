package db

import (
	"database/sql"
	"fmt"

	"github.com/rs/xid"
)

// EmptyInMemory returns an adapter to a new in-memory database with all tables created.
func EmptyInMemory() (*Adapter, error) {
	id := xid.New()
	sdb, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&_fk=on", id.String()))
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

// EmptyDisk returns an adapter to a new database, at the given file location, with
// all tables created.
func EmptyDisk(filename string) (*Adapter, error) {
	sdb, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_fk=on", filename))
	if err != nil {
		return nil, fmt.Errorf("connecting to SQLite instance: %v", err)
	}
	if err := sdb.Ping(); err != nil {
		return nil, fmt.Errorf("pinging SQLite instance: %v", err)
	}
	dba := NewAdapter(sdb)
	if err := dba.InitSchema(); err != nil {
		return nil, fmt.Errorf("creating db schema: %v", err)
	}
	return dba, nil
}
