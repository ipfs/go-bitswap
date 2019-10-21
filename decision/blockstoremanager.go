package decision

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	process "github.com/jbenet/goprocess"
)

// blockstoreManager maintains a pool of workers that make requests to the blockstore.
type blockstoreManager struct {
	bs bstore.Blockstore
	// workerLock sync.Mutex
	workerCount int
	jobs        chan func()
}

// newBlockstoreManager creates a new blockstoreManager with the given context
// and number of workers
func newBlockstoreManager(ctx context.Context, bs bstore.Blockstore, workerCount int) *blockstoreManager {
	return &blockstoreManager{
		bs:          bs,
		workerCount: workerCount,
		jobs:        make(chan func(), 1),
	}
}

func (bsm *blockstoreManager) start(ctx context.Context, px process.Process) {
	// Start up workers
	for i := 0; i < bsm.workerCount; i++ {
		i := i
		px.Go(func(px process.Process) {
			bsm.worker(ctx, i)
		})
	}
}

func (bsm *blockstoreManager) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-bsm.jobs:
			job()
		}
	}
}

func (bsm *blockstoreManager) addJob(job func()) {
	bsm.jobs <- job
}

func (bsm *blockstoreManager) getBlockSizes(ks []cid.Cid) map[cid.Cid]int {
	res := make(map[cid.Cid]int)
	if len(ks) == 0 {
		return res
	}

	var lk sync.Mutex
	bsm.jobPerKey(ks, func(c cid.Cid) {
		size, err := bsm.bs.GetSize(c)
		if err != nil {
			if err != bstore.ErrNotFound {
				log.Warningf("blockstore.GetSize(%s) error: %s", c, err)
			}
			size = 0
		}

		lk.Lock()
		res[c] = size
		lk.Unlock()
	})

	return res
}

func (bsm *blockstoreManager) getBlocks(ks []cid.Cid) map[cid.Cid]blocks.Block {
	res := make(map[cid.Cid]blocks.Block)
	if len(ks) == 0 {
		return res
	}

	var lk sync.Mutex
	bsm.jobPerKey(ks, func(c cid.Cid) {
		blk, err := bsm.bs.Get(c)
		if err != nil {
			blk = nil
			if err != bstore.ErrNotFound {
				log.Warningf("blockstore.Get(%s) error: %s", c, err)
			}
		}

		lk.Lock()
		res[c] = blk
		lk.Unlock()
	})

	return res
}

func (bsm *blockstoreManager) jobPerKey(ks []cid.Cid, jobFn func(c cid.Cid)) {
	wg := sync.WaitGroup{}
	for _, k := range ks {
		c := k
		wg.Add(1)
		bsm.addJob(func() {
			jobFn(c)
			wg.Done()
		})
	}
	wg.Wait()
}
