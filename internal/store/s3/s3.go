package s3

import (
	"context"
	"errors"
	"io"

	"github.com/iotafs/iotafs/internal/log"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/minio/minio-go/v6"
)

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	PathStyle bool
	SSL       bool
}

// Store implements the Store interface for an S3-compatible backend.
type Store struct {
	client *minio.Client
}

type s3File struct {
	r         *io.PipeReader
	w         *io.PipeWriter
	cancel    context.CancelFunc
	errc      <-chan error
	cancelled bool
	closed    bool
}

func (f *s3File) Write(p []byte) (int, error) {
	return f.w.Write(p)
}

func (f *s3File) Close() error {
	if f.cancelled {
		return errors.New("file is cancelled")
	}
	if f.closed {
		return errors.New("file already closed")
	}
	f.closed = true
	if err := f.w.Close(); err != nil {
		f.cancel()
		return err
	}
	return <-f.errc
}

func (f *s3File) Cancel() error {
	if f.closed {
		return errors.New("file is closed")
	}
	if f.cancelled {
		return errors.New("file already cancelled")
	}
	f.cancel()
	f.cancelled = true
	if err := f.w.Close(); err != nil {
		return err
	}
	err := <-f.errc
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// New creates a new client for accessing an S3-backed store.
func New(cfg Config) (*Store, error) {
	client, err := minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.SSL)
	if err != nil {
		return nil, err
	}
	return &Store{client}, nil
}

func (s *Store) NewFile(bucket string, key string) store.WriteCanceller {
	r, w := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := s.client.PutObjectWithContext(ctx, bucket, key, r, -1, minio.PutObjectOptions{})
		log.OnError(r.Close)
		errc <- err
		close(errc)
	}()
	return &s3File{r, w, cancel, errc, false, false}
}

func (s *Store) Copy(bucket string, from string, to string) error {
	src := minio.NewSourceInfo(bucket, from, nil)
	dst, err := minio.NewDestinationInfo(bucket, to, nil, nil)
	if err != nil {
		return err
	}
	return s.client.CopyObject(dst, src)
}

func (s *Store) Delete(bucket string, key string) error {
	return s.client.RemoveObject(bucket, key)
}
