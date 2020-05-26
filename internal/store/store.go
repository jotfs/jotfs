package store

import (
	"context"
	"io"
	"time"
)

// Store is an interface to an object store.
type Store interface {
	Put(ctx context.Context, bucket string, key string, r io.Reader) error

	// Copy makes a copy of a file. Returns an error if the file does not exist.
	Copy(bucket string, from string, to string) error

	// Delete deletes a file. Returns an error if the file does not exist.
	Delete(bucket string, key string) error

	// PresignGetURL generates a URL to download an object.
	PresignGetURL(bucket string, key string, expires time.Duration, contentRange *Range) (string, error)
}

// Range specifies a byte range, inclusive at each end
type Range struct {
	From uint64
	To   uint64
}

// Presigner creates pre-signed URLs for retrieving data from the store.
type Presigner interface {
}
