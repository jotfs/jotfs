package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/iotafs/iotafs/internal/log"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/minio/minio-go/v6"
)

// Config stores the configuration for the S3 store.
type Config struct {
	Region     string
	Endpoint   string
	AccessKey  string
	SecretKey  string
	PathStyle  bool
	DisableSSL bool
}

// Store implements the Store interface for an S3-compatible backend.
type Store struct {
	cfg    Config
	client *minio.Client
	svc    *s3.S3
}

// s3File implements the store File interface.
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

// Close commits all data written to the file, and saves the object to the store.
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

// Cancel discards any data written to the file. Subsequent writes to the file will fail.
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
	client, err := minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, !cfg.DisableSSL)
	if err != nil {
		return nil, err
	}
	acfg := aws.Config{
		Endpoint:         &cfg.Endpoint,
		S3ForcePathStyle: &cfg.PathStyle,
		DisableSSL:       &cfg.DisableSSL,
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
	}
	sess, err := session.NewSession(&acfg)
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	return &Store{cfg, client, svc}, nil
}

// NewFile creates a new file in the store. It is not commited until Close is called.
func (s *Store) NewFile(bucket string, key string) store.File {
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

// Copy makes a copy of an object.
func (s *Store) Copy(bucket string, from string, to string) error {
	src := minio.NewSourceInfo(bucket, from, nil)
	dst, err := minio.NewDestinationInfo(bucket, to, nil, nil)
	if err != nil {
		return err
	}
	return s.client.CopyObject(dst, src)
}

// Delete removes an object.
func (s *Store) Delete(bucket string, key string) error {
	return s.client.RemoveObject(bucket, key)
}

// func (s *Store) PutObject(ctx context.Context, r io.Reader, bucket string, key string, contentMD5 []byte) error {
// 	uploader := s3manager.NewUploaderWithClient(s.svc)
// 	uploader.Concurrency = 1

// 	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
// 		Bucket:     &bucket,
// 		Key:        &key,
// 		Body:       r,
// 		ContentMD5: aws.String(base64.StdEncoding.EncodeToString(contentMD5)),
// 	})
// 	return err
// }

// PresignGetURL returns a URL to GET an object in the store.
func (s *Store) PresignGetURL(bucket string, key string, expires time.Duration, contentRange *store.Range) (string, error) {
	var rnge *string
	if contentRange != nil {
		x := fmt.Sprintf("bytes=%d-%d", contentRange.From, contentRange.To)
		rnge = &x
	}

	req, _ := s.svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  rnge,
	})

	return req.Presign(expires)
}

// func (s *Store) PresignPutURL(bucket string, key string, expire time.Duration, contentMD5 string, contentLength int) (string, error) {
// 	// TODO: can't use minio's presign here because the public API doesn't support
// 	// signing Range headers. Temporarily using aws-sdk-go instead.
// 	cfg := aws.Config{
// 		Endpoint:         &s.cfg.Endpoint,
// 		S3ForcePathStyle: &s.cfg.PathStyle,
// 		DisableSSL:       &s.cfg.DisableSSL,
// 		Credentials:      credentials.NewStaticCredentials(s.cfg.AccessKey, s.cfg.SecretKey, ""),
// 	}
// 	sess, err := session.NewSession(&cfg)
// 	if err != nil {
// 		return "", err
// 	}
// 	svc := s3.New(sess)

// 	req, _ := svc.PutObjectRequest(&s3.PutObjectInput{
// 		Bucket: &bucket,
// 		Key:    &key,
// 	})
// 	req.HTTPRequest.Header.Set("Content-MD5", contentMD5)
// 	req.HTTPRequest.Header.Set("Content-Length", fmt.Sprintf("%d", contentLength))

// 	return req.Presign(expire)
// }

// func (s *Store) PutObject(ctx context.Context, r io.ReadSeeker, bucket string, key string) error {
// 	if _, err := r.Seek(0, io.SeekStart); err != nil {
// 		return err
// 	}
// 	hash := md5.New()
// 	_, err := io.Copy(hash, r)
// 	if err != nil {
// 		return err
// 	}
// 	if _, err := r.Seek(0, io.SeekStart); err != nil {
// 		return err
// 	}
// 	md5s := base64.StdEncoding.EncodeToString(hash.Sum(nil))

// 	_, err = s.svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
// 		Body:       r,
// 		Bucket:     &bucket,
// 		Key:        &key,
// 		ContentMD5: &md5s,
// 	})
// 	return err
// }
