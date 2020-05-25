package testutil

import (
	"time"

	"github.com/iotafs/iotafs/internal/store"
)

type MockStore struct{}

type discard struct{}

func (w discard) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w discard) Close() error {
	return nil
}

func (w discard) Cancel() error {
	return nil
}

func (s MockStore) NewFile(bucket string, key string) store.File {
	return discard{}
}

func (s MockStore) Delete(bucket string, key string) error {
	return nil
}

func (s MockStore) Copy(bucket string, from string, to string) error {
	return nil
}

func (s MockStore) PresignGetURL(bucket string, key string, expires time.Duration, contentRange *store.Range) (string, error) {
	return "", nil
}
