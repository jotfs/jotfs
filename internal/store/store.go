package store

import (
	"io"
)

// WriteCanceller is a WriteCloser with the ability to cancel all write operations before
// it is closed.
type WriteCanceller interface {
	io.WriteCloser
	Cancel() error
}

// Store is an interface to an object store.
type Store interface {
	// NewFile initializes a new file in the store. The writer must be successfully closed
	// before the file is created.
	NewFile(bucket string, key string) WriteCanceller

	// Copy makes a copy of a file in the store. Returns an error if the file does not exist.
	Copy(bucket string, from string, to string) error

	// Delete deletes a file in the store.
	Delete(bucket string, key string) error
}
