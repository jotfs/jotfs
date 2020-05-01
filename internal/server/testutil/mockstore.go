package testutil

import "github.com/iotafs/iotafs/internal/store"

type MockStore struct{}

type discard struct {
}

func (w discard) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w discard) Close() error {
	return nil
}

func (w discard) Cancel() error {
	return nil
}

func (s MockStore) NewFile(bucket string, key string) store.WriteCanceller {
	return discard{}
}

func (s MockStore) Delete(bucket string, key string) error {
	return nil
}

func (s MockStore) Copy(bucket string, from string, to string) error {
	return nil
}
