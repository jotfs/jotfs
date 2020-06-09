package s3

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jotfs/jotfs/internal/store"
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
	cfg Config
	svc *s3.S3
}

// New creates a new client for accessing an S3-backed store.
func New(cfg Config) (*Store, error) {
	acfg := aws.Config{
		Endpoint:         &cfg.Endpoint,
		S3ForcePathStyle: &cfg.PathStyle,
		DisableSSL:       &cfg.DisableSSL,
		Region:           &cfg.Region,
	}
	if cfg.AccessKey != "" {
		acfg.Credentials = credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, "")
	} else {
		fmt.Println("using shared credentials")
		acfg.Credentials = credentials.NewSharedCredentials("", "")
	}
	sess, err := session.NewSession(&acfg)
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	return &Store{cfg, svc}, nil
}

// Put saves an object to S3.
func (s *Store) Put(ctx context.Context, bucket string, key string, r io.Reader) error {
	uploader := s3manager.NewUploaderWithClient(s.svc, func(u *s3manager.Uploader) {
		u.Concurrency = 1
	})
	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:   r,
		Bucket: &bucket,
		Key:    &key,
	})
	return err
}

// Get returns an object from the store as an io.ReadCloser. Returns store.ErrNotFound
// if the object does not exist.
func (s *Store) Get(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	resp, err := s.svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil, store.ErrNotFound
		}
	}
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Copy makes a copy of an object.
func (s *Store) Copy(bucket string, from string, to string) error {
	_, err := s.svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     &bucket,
		CopySource: aws.String(path.Join(bucket, from)),
		Key:        &to,
	})
	return err
}

// Delete removes an object. No error is returned if the object does not exist.
func (s *Store) Delete(bucket string, key string) error {
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	return err
}

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
