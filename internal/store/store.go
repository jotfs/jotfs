package store

import (
	"io"
	"time"
)

// WriteCanceller is a WriteCloser with the ability to cancel all write operations before
// it is closed.
type WriteCanceller interface {
	io.WriteCloser
	Cancel() error
}

// Store is an interface to an object store.
type Store interface {
	// NewFile initializes a new file. The writer must be successfully closed before the
	// file is created.
	NewFile(bucket string, key string) WriteCanceller

	// Copy makes a copy of a file. Returns an error if the file does not exist.
	Copy(bucket string, from string, to string) error

	// Delete deletes a file. Returns an error if the file does not exist.
	Delete(bucket string, key string) error
}

// Range specifies a byte range, inclusive at each end
type Range struct {
	From int
	To   int
}

// Presigner creates pre-signed URLs for retrieving data from the store.
type Presigner interface {
	PresignGetURL(bucket string, key string, expires time.Duration, contentRange *Range) (string, error)
}
