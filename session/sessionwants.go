package session

import (
	"math/rand"
	"sync"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type sessionWants struct {
	sync.RWMutex
	toFetch   *cidQueue
	liveWants map[cid.Cid]time.Time
}

func newSessionWants(bpm *bsbpm.BlockPresenceManager) sessionWants {
	return sessionWants{
		toFetch:   newCidQueue(),
		liveWants: make(map[cid.Cid]time.Time),
	}
}

// func (sw *sessionWants) String() string {
// 	return fmt.Sprintf("%d past / %d pending / %d live", sw.pastWants.Len(), sw.toFetch.Len(), len(sw.liveWants))
// }

// ReceiveFrom moves received block CIDs from live to past wants and
// measures latency. It returns the CIDs of blocks that were actually
// wanted (as opposed to duplicates) and the total latency for all incoming blocks.
func (sw *sessionWants) ReceiveFrom(p peer.ID, ks []cid.Cid) ([]cid.Cid, time.Duration) {
	wanted := make([]cid.Cid, 0, len(ks))
	totalLatency := time.Duration(0)
	if len(ks) == 0 {
		return wanted, totalLatency
	}

	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	for _, c := range ks {
		if sw.unlockedIsWanted(c) {
			wanted = append(wanted, c)

			sentAt, ok := sw.liveWants[c]
			if ok && !sentAt.IsZero() {
				totalLatency += now.Sub(sentAt)
			}

			// Remove the CID from the live wants / toFetch queue and add it
			// to the past wants
			delete(sw.liveWants, c)
			sw.toFetch.Remove(c)
		}
	}

	return wanted, totalLatency
}

// GetNextWants adds any new wants to the list of CIDs to fetch, then moves as
// many CIDs from the fetch queue to the live wants list as possible (given the
// limit). Returns the newly live wants.
func (sw *sessionWants) GetNextWants(limit int, newWants []cid.Cid) []cid.Cid {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	// Add new wants to the fetch queue
	for _, k := range newWants {
		sw.toFetch.Push(k)
	}

	// Move CIDs from fetch queue to the live wants queue (up to the limit)
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

func (sw *sessionWants) WantsSent(ks []cid.Cid) {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	for _, c := range ks {
		if _, ok := sw.liveWants[c]; !ok {
			sw.toFetch.Remove(c)
			sw.liveWants[c] = now
		}
	}
}

// PrepareBroadcast saves the current time for each live want and returns the
// live want CIDs.
func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
		sw.liveWants[c] = now
	}
	return live
}

// CancelPending removes the given CIDs from the fetch queue.
func (sw *sessionWants) CancelPending(keys []cid.Cid) {
	sw.Lock()
	defer sw.Unlock()

	for _, k := range keys {
		sw.toFetch.Remove(k)
	}
}

func (sw *sessionWants) BlocksRequested(newWants []cid.Cid) {
	sw.Lock()
	defer sw.Unlock()

	for _, k := range newWants {
		sw.toFetch.Push(k)
	}
}

// LiveWants returns a list of live wants
func (sw *sessionWants) LiveWants() []cid.Cid {
	sw.RLock()
	defer sw.RUnlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
	}
	return live
}

func (sw *sessionWants) RandomLiveWant() cid.Cid {
	i := rand.Uint64()

	sw.RLock()
	defer sw.RUnlock()

	if len(sw.liveWants) == 0 {
		return cid.Cid{}
	}
	i %= uint64(len(sw.liveWants))
	// picking a random live want
	for k := range sw.liveWants {
		if i == 0 {
			return k
		}
		i--
	}
	return cid.Cid{}
}

// Has live wants indicates if there are any live wants
func (sw *sessionWants) HasLiveWants() bool {
	sw.RLock()
	defer sw.RUnlock()

	return len(sw.liveWants) > 0
}

// FilterWanted filters the list of cids for those that the session is
// expecting to receive
func (sw *sessionWants) FilterWanted(ks []cid.Cid) []cid.Cid {
	sw.RLock()
	defer sw.RUnlock()

	var wanted []cid.Cid
	for _, k := range ks {
		if sw.unlockedIsWanted(k) {
			wanted = append(wanted, k)
		}
	}

	return wanted
}

func (sw *sessionWants) unlockedIsWanted(c cid.Cid) bool {
	_, ok := sw.liveWants[c]
	if !ok {
		ok = sw.toFetch.Has(c)
	}
	return ok
}
