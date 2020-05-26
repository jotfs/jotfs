package server

import (
	"time"

	"github.com/iotafs/iotafs/internal/store"
)

type mockStore struct{}

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

func (s mockStore) NewFile(bucket string, key string) store.File {
	return discard{}
}

func (s mockStore) Delete(bucket string, key string) error {
	return nil
}

func (s mockStore) Copy(bucket string, from string, to string) error {
	return nil
}

func (s mockStore) PresignGetURL(bucket string, key string, expires time.Duration, contentRange *store.Range) (string, error) {
	return "", nil
}
