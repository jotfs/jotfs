package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

// ErrNotFound is returned when an object in the store could not be found.
var ErrNotFound = errors.New("not found")

// Store is an interface to an object store.
type Store interface {
	Put(ctx context.Context, bucket string, key string, r io.Reader) error

	Get(ctx context.Context, bucket string, key string) (io.ReadCloser, error)

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

// GetObject is a wrapper around the store Get method. It returns the contents of an
// object as a byte slice.
func GetObject(ctx context.Context, s Store, bucket string, key string) ([]byte, error) {
	r, err := s.Get(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, mergeErrors(err, r.Close())
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	return b, nil
}

func mergeErrors(err error, minor error) error {
	if err == nil && minor == nil {
		return nil
	}
	if err == nil {
		return minor
	}
	if minor == nil {
		return err
	}
	return fmt.Errorf("%w; %v", err, minor)
}
