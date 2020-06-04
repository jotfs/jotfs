package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/iotafs/iotafs/internal/db"
	"github.com/iotafs/iotafs/internal/object"
	"github.com/iotafs/iotafs/internal/store"
	"github.com/iotafs/iotafs/internal/sum"
)

func (srv *Server) runVacuum(ctx context.Context, createdBefore time.Time) error {
	zrs, err := srv.db.GetZeroRefcount(createdBefore.UTC())
	if err != nil {
		return fmt.Errorf("db GetZeroRefcount: %w", err)
	}

	for _, zr := range zrs {
		index, err := getPackIndex(ctx, srv.store, srv.cfg.Bucket, zr.PackID)
		if err != nil {
			return err
		}
		if len(index.Blocks) != len(zr.Sequences) {
			// Only some of the blocks in the packfile have a zero refcount. Create a
			// new packfile containing only the blocks with refcount > 0.
			if err := srv.rebuildPackfile(ctx, zr, index); err != nil {
				return fmt.Errorf("rebuilding pack index %x: %w", index.Sum, err)
			}
		}

		// Remove the old index and packfile from the store
		oldIKey := index.Sum.AsHex() + ".index"
		oldPKey := index.Sum.AsHex() + ".pack"
		err1 := srv.store.Delete(srv.cfg.Bucket, oldIKey)
		err2 := srv.store.Delete(srv.cfg.Bucket, oldPKey)
		if err1 != nil {
			err1 = fmt.Errorf("deleting %s: %w", oldIKey, err)
		}
		if err2 != nil {
			err2 = fmt.Errorf("deleting %s: %w", oldPKey, err)
		}
		err = mergeErrors(err1, err2)
		if err != nil {
			return err
		}

		srv.db.DeletePackIndex(index.Sum)
		srv.logger.Debug().Msgf("vacuum deleted packfile %x", index.Sum)
	}

	return nil
}

// getPackIndex gets a pack index from the store.
func getPackIndex(ctx context.Context, s store.Store, bucket string, sum sum.Sum) (object.PackIndex, error) {
	ikey := sum.AsHex() + ".index"
	b, err := store.GetObject(ctx, s, bucket, ikey)
	if err != nil {
		return object.PackIndex{}, fmt.Errorf("getting object %s: %w", ikey, err)
	}
	index := &object.PackIndex{}
	index.UnmarshalBinary(b)
	return *index, nil
}

func (srv *Server) rebuildPackfile(ctx context.Context, zr db.ZeroRefcount, index object.PackIndex) error {
	start := time.Now()
	bucket := srv.cfg.Bucket

	// Create a new packfile from the current one, discarding chunks with zero refcount,
	// and save it in a local tmp file.
	remove := make(map[uint64]bool, len(zr.Sequences))
	for _, i := range zr.Sequences {
		remove[i] = true
	}
	filter := func(i uint64) bool {
		_, ok := remove[i]
		return !ok
	}
	hash, err := sum.New()
	if err != nil {
		return err
	}
	r, err := srv.store.Get(ctx, bucket, index.Sum.AsHex()+".pack")
	if err != nil {
		return fmt.Errorf("store get: %w", err)
	}
	f, err := ioutil.TempFile("", "iota-")
	if err != nil {
		err = mergeErrors(err, f.Close())
		return mergeErrors(err, r.Close())
	}
	tmpName := f.Name()
	defer func() {
		if err := os.Remove(tmpName); err != nil {
			srv.logger.Error().Msgf("rebuildPackfile: %v", err)
		}
	}()
	w := io.MultiWriter(f, hash)
	size, err := object.FilterPackfile(r, w, filter)
	if err != nil {
		err = mergeErrors(fmt.Errorf("filtering packfile: %w", err), f.Close())
		return mergeErrors(err, r.Close())
	}
	if err = r.Close(); err != nil {
		return mergeErrors(err, f.Close())
	}
	if err = f.Close(); err != nil {
		return err
	}

	// Construct the index for the new packfile from the old one and save it to the store.
	// m is a mapping from sequence numbers in the new index to sequence numbers in the
	// old index
	m := make(map[uint64]uint64, 0)
	blocks := make([]object.BlockInfo, 0, len(index.Blocks)-len(remove))
	offset := index.Blocks[0].Offset
	var seq uint64
	for _, block := range index.Blocks {
		if !filter(block.Sequence) {
			continue
		}
		m[seq] = block.Sequence
		block.Offset = offset
		block.Sequence = seq
		blocks = append(blocks, block)

		offset += block.Size
		seq++
	}
	newIndex := object.PackIndex{Blocks: blocks, Sum: hash.Sum(), Size: size}
	newIKey := newIndex.Sum.AsHex() + ".index"
	buf := bytes.NewReader(newIndex.MarshalBinary())
	if err = srv.store.Put(ctx, bucket, newIKey, buf); err != nil {
		return fmt.Errorf("saving %s to store: %w", newIKey, err)
	}

	// Upload the new packfile to the store
	f, err = os.Open(tmpName)
	defer f.Close()
	if err != nil {
		return err
	}
	newPKey := newIndex.Sum.AsHex() + ".pack"
	if err := srv.store.Put(ctx, bucket, newPKey, f); err != nil {
		err = fmt.Errorf("saving %s to store: %w", newPKey, err)
		return mergeErrors(err, srv.store.Delete(bucket, newIKey))
	}

	createdAt := time.Now().UTC()
	if err := srv.db.UpdateIndex(newIndex, createdAt, index.Sum, m); err != nil {
		err = fmt.Errorf("db UpdateIndex: %w", err)
		err = mergeErrors(err, srv.store.Delete(bucket, newIKey))
		return mergeErrors(err, srv.store.Delete(bucket, newPKey))
	}

	srv.logger.Debug().
		Int64("elapsed", time.Since(start).Milliseconds()).
		Msgf("vacuum replaced packfile %x with packfile %x", index.Sum, newIndex.Sum)

	return nil
}
