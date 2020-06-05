package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/iotafs/iotafs/internal/store"

	"github.com/stretchr/testify/assert"
)

const bucket = "iotafs-testing-s3"

var cfg = Config{
	Endpoint:   "localhost:9000",
	AccessKey:  "minioadmin",
	SecretKey:  "minioadmin",
	DisableSSL: true,
	PathStyle:  true,
}

var s *Store

func TestMain(m *testing.M) {
	var err error
	s, err = New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err = s.makeBucket(bucket); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	if err := s.dropBucket(bucket); err != nil {
		log.Fatalf("dropping bucket %s: %v\n", bucket, err)
	}
	os.Exit(code)
}

func TestImplements(t *testing.T) {
	// Ensure the S3 Store implements the Store interface
	assert.Implements(t, (*store.Store)(nil), new(Store))
}

func TestPut(t *testing.T) {
	ctx := context.Background()

	// Simple file
	k0 := randKey()
	data := []byte("Hello world!")
	err := s.Put(ctx, bucket, k0, bytes.NewReader(data))
	assert.NoError(t, err)
	// assert.NoError(t, s.Delete(bucket, k0))

	// Get object
	r, err := s.Get(ctx, bucket, k0)
	assert.NoError(t, err)
	dataGet, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, data, dataGet)

	// Error if object doesn't exist
	_, err = s.Get(ctx, bucket, "does-not-exist")
	assert.Equal(t, store.ErrNotFound, err)

	// Delete
	err = s.Delete(bucket, k0)
	assert.NoError(t, err)

	// No error if delete same object again
	err = s.Delete(bucket, k0)
	assert.NoError(t, err)

	// No error if delete object which doesn't exist
	err = s.Delete(bucket, k0)
	assert.NoError(t, err)
}

func TestCopy(t *testing.T) {
	ctx := context.Background()

	// Create file
	k0 := randKey()
	err := s.Put(ctx, bucket, k0, bytes.NewReader([]byte("Hello world!")))
	assert.NoError(t, err)
	defer s.Delete(bucket, k0)

	// Make a copy of the file
	kCopy := k0 + "-copy"
	err = s.Copy(bucket, k0, kCopy)
	assert.NoError(t, err)
	assert.NoError(t, s.Delete(bucket, kCopy))

	// Make copy of non-existent file
	err = s.Copy(bucket, "i-never-existed", kCopy)
	assert.Error(t, err)
}

func TestGetPresignedURL(t *testing.T) {
	ctx := context.Background()

	// Write a test file
	k := "test.txt"
	b := []byte(strings.Repeat("Hello World!\n", 100))
	err := s.Put(ctx, bucket, k, bytes.NewReader(b))
	assert.NoError(t, err)

	// Generate a presigned GET url for the first 100 bytes of the file
	rnge := store.Range{From: 0, To: 99}
	url, err := s.PresignGetURL(bucket, k, time.Duration(5*time.Minute), &rnge)
	fmt.Println(url)
	assert.NotEmpty(t, url)
	assert.NoError(t, err)

	// GET data from the URL
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", rnge.From, rnge.To))
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, b[rnge.From:rnge.To+1], body)
}

// randKey generates a random object key.
func randKey() string {
	b := make([]byte, 10)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Store) dropBucket(name string) error {
	resp, err := s.svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: &name,
	})
	if err != nil {
		return err
	}
	for _, obj := range resp.Contents {
		if err = s.Delete(name, *obj.Key); err != nil {
			return err
		}
	}
	_, err = s.svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: &name,
	})
	return err
}

func (s *Store) makeBucket(name string) error {
	_, err := s.svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &name,
	})
	return err
}
