package decision

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"

	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	process "github.com/jbenet/goprocess"
)

func TestBlockstoreManagerNotFoundKey(t *testing.T) {
	ctx := context.Background()
	bsdelay := delay.Fixed(3 * time.Millisecond)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	bsm := newBlockstoreManager(ctx, bstore, 5)
	bsm.start(ctx, process.WithTeardown(func() error { return nil }))

	cids := testutil.GenerateCids(4)
	sizes := bsm.getBlockSizes(cids)
	if len(sizes) != len(cids) {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		if sizes[c] != 0 {
			t.Fatal("Non-existent block should have size 0")
		}
	}

	blks := bsm.getBlocks(cids)
	if len(blks) != len(cids) {
		t.Fatal("Wrong response length")
	}

	for _, b := range blks {
		if b != nil {
			t.Fatal("Non-existent block should be nil")
		}
	}
}

func TestBlockstoreManager(t *testing.T) {
	ctx := context.Background()
	bsdelay := delay.Fixed(3 * time.Millisecond)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	bsm := newBlockstoreManager(ctx, bstore, 5)
	bsm.start(ctx, process.WithTeardown(func() error { return nil }))

	exp := make(map[cid.Cid]blocks.Block)
	var blks []blocks.Block
	for i := 0; i < 32; i++ {
		buf := make([]byte, 1024*(i+1))
		_, _ = rand.Read(buf)
		b := blocks.NewBlock(buf)
		blks = append(blks, b)
		exp[b.Cid()] = b
	}
	if err := bstore.PutMany(blks); err != nil {
		t.Fatal(err)
	}

	var cids []cid.Cid
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	sizes := bsm.getBlockSizes(cids)
	if len(sizes) != len(cids) {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		expSize := len(exp[c].RawData())
		if sizes[c] != expSize {
			t.Fatal("Block should have size ", expSize)
		}
	}

	fetched := bsm.getBlocks(cids)
	if len(fetched) != len(cids) {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		if fetched[c].Cid() != c {
			t.Fatal("Block should have cid ", c)
		}
	}
}

func TestBlockstoreManagerConcurrency(t *testing.T) {
	ctx := context.Background()
	bsdelay := delay.Fixed(3 * time.Millisecond)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	workerCount := 5
	bsm := newBlockstoreManager(ctx, bstore, workerCount)
	bsm.start(ctx, process.WithTeardown(func() error { return nil }))

	blks := testutil.GenerateBlocksOfSize(32, 8*1024)
	var ks []cid.Cid
	for _, b := range blks {
		ks = append(ks, b.Cid())
	}

	// Create more concurrent requests than the number of workers
	wg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			sizes := bsm.getBlockSizes(ks)
			if len(sizes) != len(ks) {
				t.Fatal("Wrong response length")
			}

			for _, c := range ks {
				if sizes[c] != 0 {
					t.Fatal("Non-existent block should have size 0")
				}
			}
		}()
	}
	wg.Wait()
}
