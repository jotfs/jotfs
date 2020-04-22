package s3

import (
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

	"github.com/iotafs/iotafs/internal/store"

	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
)

const bucket = "iotafs-testing"

func cfg() Config {
	return Config{
		Endpoint:  "localhost:9000",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
	}
}

func TestMain(m *testing.M) {
	cfg := cfg()
	client, err := minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.SSL)
	if err != nil {
		log.Fatalf("connecting to server: %v\n", err)
	}
	if err := client.MakeBucket(bucket, ""); err != nil {
		log.Fatalf("creating bucket %s: %v\n", bucket, err)
	}
	code := m.Run()
	if err := dropBucket(client, bucket); err != nil {
		log.Fatalf("dropping bucket %s: %v\n", bucket, err)
	}
	os.Exit(code)
}

func TestImplements(t *testing.T) {
	// Ensure the S3 Store implements the Store interface
	assert.Implements(t, (*store.Store)(nil), new(Store))
}

func TestNew(t *testing.T) {
	store, err := New(cfg())
	assert.NoError(t, err)
	assert.NotNil(t, store)

	// Simple file
	k0 := randKey()
	f0 := store.NewFile(bucket, k0)
	f0.Write([]byte("Hello world!"))
	err = f0.Close()
	assert.NoError(t, err)
	assert.NoError(t, store.Delete(bucket, k0))

	// Empty file
	k1 := randKey()
	f1 := store.NewFile(bucket, k1)
	err = f1.Close()
	assert.NoError(t, err)
	assert.NoError(t, store.Delete(bucket, k1))
}

func TestCopy(t *testing.T) {
	store, err := New(cfg())
	assert.NoError(t, err)

	// Create file
	k0 := randKey()
	f0 := store.NewFile(bucket, k0)
	_, err = f0.Write([]byte("Hello world!"))
	assert.NoError(t, err)
	err = f0.Close()
	assert.NoError(t, err)
	defer store.Delete(bucket, k0)

	// Make a copy of the file
	kCopy := k0 + "-copy"
	err = store.Copy(bucket, k0, kCopy)
	assert.NoError(t, err)
	assert.NoError(t, store.Delete(bucket, kCopy))

	// Make copy of non-existent file
	err = store.Copy(bucket, "i-never-existed", kCopy)
	assert.Error(t, err)
}

func TestCancel(t *testing.T) {
	store, err := New(cfg())
	assert.NoError(t, err)

	// Create file and write some data
	k0 := randKey()
	f0 := store.NewFile(bucket, k0)
	_, err = f0.Write([]byte("Hello world!"))
	assert.NoError(t, err)

	// Cancel the file
	err = f0.Cancel()
	assert.NoError(t, err)

	// Close after cancel should return error
	err = f0.Close()
	assert.Error(t, err)

	// Cancel again should return error
	err = f0.Cancel()
	assert.Error(t, err)
}

// randKey generates a random object key.
func randKey() string {
	b := make([]byte, 10)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// dropBucket deletes a bucket and its contents.
func dropBucket(client *minio.Client, bucket string) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	for obj := range client.ListObjectsV2(bucket, "", true, doneCh) {
		if err := client.RemoveObject(bucket, obj.Key); err != nil {
			return err
		}
	}
	return client.RemoveBucket(bucket)
}

func TestGetPresignedURL(t *testing.T) {
	s, err := New(cfg())
	assert.NoError(t, err)

	// Write a test file
	k := "test.txt"
	b := []byte(strings.Repeat("Hello World!\n", 100))
	f := s.NewFile(bucket, k)
	_, err = f.Write(b)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	// Generate a presigned GET url for the first 100 bytes of the file
	rnge := store.Range{From: 0, To: 99}
	url, err := s.PresignGetURL(bucket, k, time.Duration(5*time.Minute), &rnge)
	assert.NoError(t, err)

	// GET the URL
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", rnge.From, rnge.To))
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, b[rnge.From:rnge.To+1], body)
}
