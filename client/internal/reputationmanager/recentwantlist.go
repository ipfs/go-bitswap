package reputationmanager

import (
	"context"
	"sync"
	"time"

	"github.com/AndreasBriese/bbloom"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type recentWantList struct {
	params   *recentWantListParams
	oldWants bbloom.Bloom
	newWants bbloom.Bloom
	lk       sync.RWMutex
}

type recentWantListParams struct {
	numEntries      float64
	wrongPositives  float64
	refreshInterval time.Duration
}

func newRecentWantList(params *recentWantListParams) *recentWantList {
	r := &recentWantList{
		params:   params,
		oldWants: bbloom.New(params.numEntries, params.wrongPositives),
		newWants: bbloom.New(params.numEntries, params.wrongPositives),
	}

	return r
}

func (r *recentWantList) loop(ctx context.Context) {
	ticker := time.NewTicker(r.params.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.swapWantLists()
		}
	}
}

func (r *recentWantList) swapWantLists() {
	r.lk.Lock()
	defer r.lk.Unlock()

	r.oldWants = r.newWants
	r.newWants = bbloom.New(r.params.numEntries, r.params.wrongPositives)
}

func (r *recentWantList) addWants(cids []cid.Cid) {
	r.lk.Lock()
	defer r.lk.Unlock()

	for _, c := range cids {
		r.newWants.Add(c.Bytes())
	}
}

func (r *recentWantList) splitRecentlyWanted(blks []blocks.Block, presences []cid.Cid) (wantedBlks []blocks.Block, unwantedBlks []blocks.Block, wantedPresences []cid.Cid, unwantedPresences []cid.Cid) {
	r.lk.RLock()
	defer r.lk.RUnlock()

	for _, blk := range blks {
		entry := blk.Cid().Bytes()
		if r.newWants.Has(entry) || r.oldWants.Has(entry) {
			wantedBlks = append(wantedBlks, blk)
		} else {
			unwantedBlks = append(unwantedBlks, blk)
		}
	}

	for _, presence := range presences {
		entry := presence.Bytes()
		if r.newWants.Has(entry) || r.oldWants.Has(entry) {
			wantedPresences = append(wantedPresences, presence)
		} else {
			unwantedPresences = append(unwantedPresences, presence)
		}
	}

	return wantedBlks, unwantedBlks, wantedPresences, unwantedPresences
}
