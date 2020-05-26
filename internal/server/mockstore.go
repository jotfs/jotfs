package server

import (
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/iotafs/iotafs/internal/store"
)

type mockStore struct{}

func (s mockStore) Put(ctx context.Context, bucket string, key string, r io.Reader) error {
	_, err := io.Copy(ioutil.Discard, r)
	return err
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
