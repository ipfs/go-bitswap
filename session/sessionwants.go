package session

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("bitswap:sw")

type sessionWants struct {
	toFetchLk   sync.RWMutex
	toFetch     *cidQueue
	pastWantsLk sync.RWMutex
	pastWants   *cidQueue
	liveWantsLk sync.RWMutex
	liveWants   map[cid.Cid]time.Time
}

func newSessionWants() *sessionWants {
	return &sessionWants{
		liveWants: make(map[cid.Cid]time.Time),
		toFetch:   newCidQueue(),
		pastWants: newCidQueue(),
	}
}

func (sw *sessionWants) Stats() string {
	return fmt.Sprintf("%d past / %d pending / %d live", sw.pastWants.Len(), sw.toFetch.Len(), len(sw.liveWants))
}

func (sw *sessionWants) BlocksReceived(cids []cid.Cid) (*cid.Set, time.Duration) {
	sw.liveWantsLk.Lock()
	defer sw.liveWantsLk.Unlock()
	sw.toFetchLk.Lock()
	defer sw.toFetchLk.Unlock()
	sw.pastWantsLk.Lock()
	defer sw.pastWantsLk.Unlock()

	totalLatency := time.Duration(0)
	wanted := sw.unlockedGetWanted(cids)
	for _, c := range cids {
		if wanted.Has(c) {
			// If the block CID was in the live wants queue, remove it
			tval, ok := sw.liveWants[c]
			if ok {
				totalLatency += time.Since(tval)
				delete(sw.liveWants, c)
			} else {
				// Otherwise remove it from the toFetch queue, if it was there
				sw.toFetch.Remove(c)
			}

			// Keep track of CIDs we've successfully fetched
			sw.pastWants.Push(c)
		}
	}

	return wanted, totalLatency
}

func (sw *sessionWants) unlockedGetWanted(cids []cid.Cid) *cid.Set {
	cset := cid.NewSet()
	for _, c := range cids {
		if sw.unlockedIsWanted(c) {
			cset.Add(c)
		}
	}
	return cset
}

func (sw *sessionWants) GetNextWants(limit int, newWants []cid.Cid) []cid.Cid {
	sw.liveWantsLk.Lock()
	defer sw.liveWantsLk.Unlock()
	sw.toFetchLk.Lock()
	defer sw.toFetchLk.Unlock()

	now := time.Now()

	for _, k := range newWants {
		sw.toFetch.Push(k)
	}

	currentLiveCount := len(sw.liveWants)
	toAdd := limit - currentLiveCount

	var live []cid.Cid
	for ; toAdd > 0 && sw.toFetch.Len() > 0; toAdd-- {
		c := sw.toFetch.Pop()
		live = append(live, c)
		sw.liveWants[c] = now
	}

	return live
}

func (sw *sessionWants) SplitIsWasWanted(cids []cid.Cid) ([]cid.Cid, []cid.Cid) {
	sw.liveWantsLk.RLock()
	defer sw.liveWantsLk.RUnlock()
	sw.toFetchLk.RLock()
	defer sw.toFetchLk.RUnlock()
	sw.pastWantsLk.RLock()
	defer sw.pastWantsLk.RUnlock()

	isWanted := make([]cid.Cid, 0, len(cids))
	wasWanted := make([]cid.Cid, 0, len(cids))
	for _, c := range cids {
		if sw.unlockedIsWanted(c) {
			isWanted = append(isWanted, c)
		} else if sw.pastWants.Has(c) {
			wasWanted = append(wasWanted, c)
		}
	}
	return isWanted, wasWanted
}

func (sw *sessionWants) IsInterested(c cid.Cid) bool {
	sw.liveWantsLk.RLock()
	defer sw.liveWantsLk.RUnlock()
	sw.toFetchLk.RLock()
	defer sw.toFetchLk.RUnlock()
	sw.pastWantsLk.RLock()
	defer sw.pastWantsLk.RUnlock()

	return sw.unlockedIsWanted(c) || sw.pastWants.Has(c)
}

func (sw *sessionWants) IsWanted(c cid.Cid) bool {
	sw.liveWantsLk.RLock()
	defer sw.liveWantsLk.RUnlock()
	sw.toFetchLk.RLock()
	defer sw.toFetchLk.RUnlock()

	return sw.unlockedIsWanted(c)
}

func (sw *sessionWants) unlockedIsWanted(c cid.Cid) bool {
	_, ok := sw.liveWants[c]
	if !ok {
		ok = sw.toFetch.Has(c)
	}
	return ok
}

func (sw *sessionWants) WasWanted(c cid.Cid) bool {
	sw.pastWantsLk.RLock()
	defer sw.pastWantsLk.RUnlock()

	return sw.pastWants.Has(c)
}

func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	sw.liveWantsLk.Lock()
	defer sw.liveWantsLk.Unlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	now := time.Now()
	for c := range sw.liveWants {
		live = append(live, c)
		sw.liveWants[c] = now
	}
	return live
}

func (sw *sessionWants) LiveWants() []cid.Cid {
	sw.liveWantsLk.RLock()
	defer sw.liveWantsLk.RUnlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
	}
	return live
}

func (sw *sessionWants) LiveWantsCount() int {
	sw.liveWantsLk.RLock()
	defer sw.liveWantsLk.RUnlock()

	return len(sw.liveWants)
}

func (sw *sessionWants) RandomLiveWant() cid.Cid {
	sw.liveWantsLk.RLock()
	defer sw.liveWantsLk.RUnlock()

	if len(sw.liveWants) == 0 {
		return cid.Cid{}
	}
	i := rand.Intn(len(sw.liveWants))
	// picking a random live want
	for k := range sw.liveWants {
		if i == 0 {
			return k
		}
		i--
	}
	return cid.Cid{}
}

func (sw *sessionWants) CancelPending(keys []cid.Cid) {
	sw.toFetchLk.Lock()
	defer sw.toFetchLk.Unlock()

	for _, k := range keys {
		sw.toFetch.Remove(k)
	}
}
