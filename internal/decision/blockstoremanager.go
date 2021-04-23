package decision

import (
	"context"
	"fmt"
	mh "github.com/multiformats/go-multihash"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	process "github.com/jbenet/goprocess"
)

// blockstoreManager maintains a pool of workers that make requests to the blockstore.
type blockstoreManager struct {
	bs          bstore.Blockstore
	workerCount int
	jobs        chan func()
	px          process.Process
}

// newBlockstoreManager creates a new blockstoreManager with the given context
// and number of workers
func newBlockstoreManager(bs bstore.Blockstore, workerCount int) *blockstoreManager {
	return &blockstoreManager{
		bs:          bs,
		workerCount: workerCount,
		jobs:        make(chan func()),
	}
}

func (bsm *blockstoreManager) start(px process.Process) {
	bsm.px = px

	// Start up workers
	for i := 0; i < bsm.workerCount; i++ {
		px.Go(func(px process.Process) {
			bsm.worker()
		})
	}
}

func (bsm *blockstoreManager) worker() {
	for {
		select {
		case <-bsm.px.Closing():
			return
		case job := <-bsm.jobs:
			job()
		}
	}
}

func (bsm *blockstoreManager) addJob(ctx context.Context, job func()) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-bsm.px.Closing():
		return fmt.Errorf("shutting down")
	case bsm.jobs <- job:
		return nil
	}
}

func (bsm *blockstoreManager) getBlockSizes(ctx context.Context, ks []cid.Cid) (map[cid.Cid]int, error) {
	res := make(map[cid.Cid]int)
	if len(ks) == 0 {
		return res, nil
	}

	var lk sync.Mutex
	return res, bsm.jobPerKey(ctx, ks, func(c cid.Cid) {
		size, err := bsm.bs.GetSize(c)
		if err != nil {
			if err != bstore.ErrNotFound {
				// Note: this isn't a fatal error. We shouldn't abort the request
				log.Errorf("blockstore.GetSize(%s) error: %s", c, err)
			} else {
				if c.Prefix().MhType == mh.IDENTITY {
					panic("failed to find IDENTITY CID")
					// Just to validate internal hypothesis that these will always
					// turn up.
					// Confirmed in https://github.com/ipfs/go-ipfs-blockstore/blob/fb07d7bc5aece18c62603f36ac02db2e853cadfa/idstore.go#L82-L85.
				}
			}
		} else {
			lk.Lock()
			res[c] = size
			lk.Unlock()
		}
	})
}

func (bsm *blockstoreManager) getBlocks(ctx context.Context, ks []cid.Cid) (map[cid.Cid]blocks.Block, error) {
	res := make(map[cid.Cid]blocks.Block)
	if len(ks) == 0 {
		return res, nil
	}

	var lk sync.Mutex
	return res, bsm.jobPerKey(ctx, ks, func(c cid.Cid) {
		blk, err := bsm.bs.Get(c)
		if err != nil {
			if err != bstore.ErrNotFound {
				// Note: this isn't a fatal error. We shouldn't abort the request
				log.Errorf("blockstore.Get(%s) error: %s", c, err)
			}
		} else {
			lk.Lock()
			res[c] = blk
			lk.Unlock()
		}
	})
}

func (bsm *blockstoreManager) jobPerKey(ctx context.Context, ks []cid.Cid, jobFn func(c cid.Cid)) error {
	var err error
	wg := sync.WaitGroup{}
	for _, k := range ks {
		c := k
		wg.Add(1)
		err = bsm.addJob(ctx, func() {
			jobFn(c)
			wg.Done()
		})
		if err != nil {
			wg.Done()
			break
		}
	}
	wg.Wait()
	return err
}
