package server

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/jotfs/jotfs/internal/store"
)

type mockStore struct {
	data map[string]map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{make(map[string]map[string][]byte, 0)}
}

func (s *mockStore) Put(ctx context.Context, bucket string, key string, r io.Reader) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	if _, ok := s.data[bucket]; !ok {
		s.data[bucket] = make(map[string][]byte, 0)
	}
	s.data[bucket][key] = data
	return nil
}

func (s *mockStore) Delete(bucket string, key string) error {
	if _, ok := s.data[bucket]; ok {
		if _, ok := s.data[bucket][key]; !ok {
			return store.ErrNotFound
		}
		delete(s.data[bucket], key)
		return nil
	}
	return store.ErrNotFound
}

func (s mockStore) Copy(bucket string, from string, to string) error {
	if _, ok := s.data[bucket]; !ok {
		return store.ErrNotFound
	}
	data, ok := s.data[bucket][from]
	if !ok {
		return store.ErrNotFound
	}
	b := make([]byte, len(data))
	copy(b, data)
	s.data[bucket][to] = data
	return nil
}

func (s mockStore) PresignGetURL(bucket string, key string, expires time.Duration, contentRange *store.Range) (string, error) {
	return "", nil
}

func (s mockStore) Get(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	if _, ok := s.data[bucket]; !ok {
		return nil, store.ErrNotFound
	}
	data, ok := s.data[bucket][key]
	if !ok {
		return nil, store.ErrNotFound
	}
	b := bytes.NewReader(data)
	return ioutil.NopCloser(b), nil
}
