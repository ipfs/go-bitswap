package session

import (
	"math/rand"
	"fmt"
	"sort"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type sessionWants struct {
	sync.RWMutex
	toFetch   *cidQueue
	liveWants map[cid.Cid]time.Time
	pastWants *cid.Set

	wantPotential map[cid.Cid]map[peer.ID]float64
	wantPeers map[cid.Cid][]peer.ID
	potentialThreshold float64

	blockPresence map[cid.Cid]map[peer.ID]bool
}

func newSessionWants() sessionWants {
	return sessionWants{
		liveWants: make(map[cid.Cid]time.Time),
		toFetch:   newCidQueue(),
		pastWants: cid.NewSet(),
		wantPotential: make(map[cid.Cid]map[peer.ID]float64),
		wantPeers: make(map[cid.Cid][]peer.ID),
		blockPresence: make(map[cid.Cid]map[peer.ID]bool),
		potentialThreshold: 1.5,
	}
}

func (sw *sessionWants) Stats() string {
	return fmt.Sprintf("%d past / %d pending / %d live", sw.pastWants.Len(), sw.toFetch.Len(), len(sw.liveWants))
}

func (sw *sessionWants) PotentialThreshold() float64 {
	sw.RLock()
	defer sw.RUnlock()

	return sw.potentialThreshold
}

func (sw *sessionWants) IncrementPotentialThreshold() {
	sw.Lock()
	defer sw.Unlock()

	sw.potentialThreshold++
}

func (sw *sessionWants) DecreasePotentialThreshold() {
	sw.Lock()
	defer sw.Unlock()

	sw.potentialThreshold = sw.potentialThreshold * 0.9
	if sw.potentialThreshold < 0.5 {
		sw.potentialThreshold = 0.5
	}
	// log.Debugf("potentialThreshold: %f\n", sw.potentialThreshold)
}

// BlocksReceived moves received block CIDs from live to past wants and
// measures latency. It returns the CIDs of blocks that were actually wanted
// (as opposed to duplicates) and the total latency for all incoming blocks.
func (sw *sessionWants) BlocksReceived(cids []cid.Cid) ([]cid.Cid, time.Duration) {
// fmt.Println("   ", sw.Stats())
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	totalLatency := time.Duration(0)
	wanted := make([]cid.Cid, 0, len(cids))
	for _, c := range cids {
		if sw.unlockedIsWanted(c) {
			wanted = append(wanted, c)

			// If the block CID was in the live wants queue, remove it
			tval, ok := sw.liveWants[c]
			if ok {
				totalLatency += now.Sub(tval)
				delete(sw.liveWants, c)
			} else {
				// Otherwise remove it from the toFetch queue, if it was there
				sw.toFetch.Remove(c)
			}

			// Keep track of CIDs we've successfully fetched
			sw.pastWants.Add(c)
			sw.clearWantPotential(c)
			sw.clearWantSentToPeer(c)
		}
	}
// fmt.Println("   ", sw.Stats())
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

// PrepareBroadcast saves the current time for each live want and returns the
// live want CIDs.
func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	// live := make([]cid.Cid, 0, len(sw.liveWants))
	live := make([]cid.Cid, 0, sw.toFetch.Len() + len(sw.liveWants))
	for _, c := range sw.toFetch.Cids() {
		live = append(live, c)
		sw.liveWants[c] = now
	}
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

// ForEachUniqDup iterates over each of the given CIDs and calls isUniqFn
// if the session is expecting a block for the CID, or isDupFn if the session
// has already received the block.
func (sw *sessionWants) ForEachUniqDup(ks []cid.Cid, isUniqFn, isDupFn func()) {
	sw.RLock()
	defer sw.RUnlock()

	for _, k := range ks {
		if sw.unlockedIsWanted(k) {
			isUniqFn()
		} else if sw.pastWants.Has(k) {
			isDupFn()
		}
	}
}

func (sw *sessionWants) BlocksRequested(newWants []cid.Cid) {
	sw.Lock()
	defer sw.Unlock()

	for _, k := range newWants {
		sw.toFetch.Push(k)
	}
}

func (sw *sessionWants) BlockInfoReceived(p peer.ID, haves []cid.Cid, dontHaves []cid.Cid) {
	sw.Lock()
	defer sw.Unlock()

	for _, c := range haves {
		// If we sent a want to this peer, decrease the want's potential by the
		// corresponding amount
		if sw.wasWantSentToPeer(c, p) {
			sw.removeWantPotential(c, p)
		}
		// If we want this block, record that the peer has the block
		if sw.unlockedIsWanted(c) {
			sw.updateBlockPresence(c, p, true)
		}
	}

	for _, c := range dontHaves {
		// If we sent a want to this peer, decrease the want's potential by the
		// corresponding amount
		if sw.wasWantSentToPeer(c, p) {
			sw.removeWantPotential(c, p)
		}
		// If we want this block, record that the peer does not have the block
		if sw.unlockedIsWanted(c) {
			sw.updateBlockPresence(c, p, false)
		}
	}
}

func (sw *sessionWants) updateBlockPresence(c cid.Cid, p peer.ID, present bool) {
	_, ok := sw.blockPresence[c]
	if !ok {
		sw.blockPresence[c] = make(map[peer.ID]bool)
	}
	sw.blockPresence[c][p] = present
}

func (sw *sessionWants) PopNextPending(peers []peer.ID) (cid.Cid, []cid.Cid, peer.ID) {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	var wantHaves []cid.Cid
	c, p, potential := sw.getBestPotentialLiveWant(peers)
	if c.Defined() {
		sw.setWantSentToPeer(c, p)
		sw.addWantPotential(c, p, potential)
		if _, ok := sw.liveWants[c]; !ok {
			sw.toFetch.Remove(c)
			sw.liveWants[c] = now
		}
		wantHaves = sw.getOtherWants(c, p)
	}

	return c, wantHaves, p
}

func (sw *sessionWants) getOtherWants(c cid.Cid, p peer.ID) []cid.Cid {
	others := make([]cid.Cid, 0)

	for _, k := range sw.toFetch.Cids() {
		if !c.Equals(k) && !sw.wasWantSentToPeer(c, p) {
			others = append(others, k)
		}
	}
	for k := range sw.liveWants {
		if !c.Equals(k) && !sw.wasWantSentToPeer(c, p) {
			others = append(others, k)
		}
	}

	return others
}

func (sw *sessionWants) wasWantSentToPeer(c cid.Cid, p peer.ID) bool {
	wps := sw.wantPeers[c]
	for _, wp := range wps {
		if wp == p {
			return true
		}
	}
	return false
}

type potentialGain struct {
	cid cid.Cid
	potential float64
	maxGain float64
	peer peer.ID
}

func (pg potentialGain) String() string {
	return fmt.Sprintf("%s potential %f. Gain for %s: %f", pg.cid.String()[2:8], pg.potential, pg.peer, pg.maxGain)
}

func potentialGainLess(pgs []potentialGain) func (i, j int) bool {
	indices := make(map[cid.Cid]int, len(pgs))
	for i, pg := range pgs {
		indices[pg.cid] = i
	}

	return func (i, j int) bool {
		// Sort by max gain, highest to lowest
		if pgs[i].maxGain > pgs[j].maxGain {
			return true
		}
		if pgs[i].maxGain < pgs[j].maxGain {
			return false
		}

		// Sort by potential, lowest to highest
		if pgs[i].potential < pgs[j].potential {
			return true
		}
		if pgs[i].potential > pgs[j].potential {
			return false
		}

		// TODO: Sort by time sent asc for live wants?

		// Sort by index, lowest to highest
		if indices[pgs[i].cid] < indices[pgs[j].cid] {
			return true
		}
		return false
	}
}

func (sw *sessionWants) getBestPotentialLiveWant(peers []peer.ID) (cid.Cid, peer.ID, float64) {
// fmt.Printf("getBestPotentialLiveWant(%s)\n", peers)
	bestC := cid.Cid{}
	bestP := peer.ID("")
	bestPotential := -1.0

	// Work out the best peer to send each want to, and how big a potential
	// would be gained
	pgs := make([]potentialGain, 0, sw.toFetch.Len() + len(sw.liveWants))
	for c := range sw.liveWants {
		potential := sw.sumWantPotential(c)
		if potential < sw.potentialThreshold {
			// log.Debugf("%s: live want potential %.2f is below threshold %.2f", c.String()[2:6], potential, sw.potentialThreshold)
			pg := sw.getPotentialGain(c, peers)
			if pg.peer != "" {
				pgs = append(pgs, pg)
			}
		} else {
			// log.Debugf("%s: live want potential %.2f is above threshold %.2f", c.String()[2:6], potential, sw.potentialThreshold)
		}
	}
	for _, c := range sw.toFetch.Cids() {
		// log.Debugf("%s: to fetch want selected (threshold %.2f)", c.String()[2:6], sw.potentialThreshold)
		// fmt.Printf("  selecting %s from fetch queue (session threshold %f)\n", c.String()[2:8], sw.potentialThreshold)
		pg := sw.getPotentialGain(c, peers)
		if pg.peer != "" {
			pgs = append(pgs, pg)
		}
	}

	if len(pgs) == 0 {
		// totalUnder := 0
		// totalWithUncontactedPeers := 0
		// for c, _ := range sw.liveWants {
		// 	potential := sw.sumWantPotential(c)
		// 	if potential < sw.potentialThreshold {
		// 		totalUnder++
		// 		candidates := sw.notInWantPeers(c, peers)
		// 		if len(candidates) > 0 {
		// 			totalWithUncontactedPeers++
		// 		}
		// 	}
		// }
		// log.Debugf("     No match of %d peers - threshold: %.2f, fetch: %d, live: %d, past: %d", len(peers), sw.potentialThreshold, sw.toFetch.Len(), len(sw.liveWants), sw.pastWants.Len())
		// log.Debugf("       Total under threshold: %d. Total with uncontacted peers: %d", totalUnder, totalWithUncontactedPeers)
		return bestC, bestP, bestPotential
	}

	sort.Slice(pgs, potentialGainLess(pgs))
	bestPg := pgs[0]
	if bestPg.maxGain >= 0 {
		bestC, bestP, bestPotential = bestPg.cid, bestPg.peer, bestPg.maxGain
	}
	log.Debugf("Best: %s (%d candidates)\n", bestPg, len(pgs))
	return bestC, bestP, bestPotential
}

// Amount to increase potential by if we received a HAVE message
const rcvdHavePotentialGain = 0.8

func (sw * sessionWants) getPotentialGain(c cid.Cid, peers []peer.ID) potentialGain {
	maxGain := -1.0
	var maxPeer peer.ID

	// Filter peers for those that we haven't yet sent a want to for this CID
	candidates := sw.notInWantPeers(c, peers)
	for _, p := range candidates {
		gain := 0.0

		// Adjust the cid / peer potential if the peer sent us a
		// HAVE or HAVE_NOT for the cid
		if peersPresence, ok := sw.blockPresence[c]; ok {
			if has, okp := peersPresence[p]; okp {
				if has {
					gain += rcvdHavePotentialGain
				} else {
					gain -= rcvdHavePotentialGain
				}
			}
		} else {
			// TODO: Take into account peer's uniq / dup ratio
			gain += 0.5
		}

		// TODO: Take into account peer's uniq / dup ratio
		betterPeer := rand.Intn(2) == 0

		if gain >= 0 && (gain > maxGain || (gain == maxGain && betterPeer)) {
			maxGain = gain
			maxPeer = p
		}
	}

	potential := sw.sumWantPotential(c)
	return potentialGain { c, potential, maxGain, maxPeer }
}

func (sw *sessionWants) notInWantPeers(cid cid.Cid, peers []peer.ID) []peer.ID {
	wps, ok := sw.wantPeers[cid]
	if !ok {
		// fmt.Printf("      notInWantPeers no record\n")
		return peers
	}

	ps := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		sentToPeer := false
		for _, wp := range wps {
			if p == wp {
				sentToPeer = true
			}
		}
		if !sentToPeer {
			ps = append(ps, p)
		}
	}
	return ps
}

func (sw *sessionWants) setWantSentToPeer(cid cid.Cid, p peer.ID) {
	wps, ok := sw.wantPeers[cid]
	if !ok {
		sw.wantPeers[cid] = make([]peer.ID, 0, 1)
	}
	sw.wantPeers[cid] = append(wps, p)
}

func (sw *sessionWants) clearWantSentToPeer(cid cid.Cid) {
	delete(sw.wantPeers, cid)
}

func (sw *sessionWants) addWantPotential(c cid.Cid, p peer.ID, gain float64) {
	// old := sw.sumWantPotential(c)
	if _, ok := sw.wantPotential[c]; !ok {
		sw.wantPotential[c] = make(map[peer.ID]float64)
	}
	sw.wantPotential[c][p] = gain
	// log.Debugf("  %s: potential %.2f -> %.2f", c.String()[2:8], old, sw.sumWantPotential(c))
}

func (sw *sessionWants) removeWantPotential(c cid.Cid, p peer.ID) {
	if _, ok := sw.wantPotential[c]; ok {
		delete(sw.wantPotential[c], p)
	}
}

func (sw *sessionWants) sumWantPotential(c cid.Cid) float64 {
	total := 0.0
	if cp, ok := sw.wantPotential[c]; ok {
		for _, amount := range cp {
			total += amount
		}
	}
	return total
}

func (sw *sessionWants) clearWantPotential(c cid.Cid) {
	delete(sw.wantPotential, c)
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

func (sw *sessionWants) LiveWantsCount() int {
	sw.RLock()
	defer sw.RUnlock()

	return len(sw.liveWants)
}

func (sw *sessionWants) PendingCount() int {
	sw.RLock()
	defer sw.RUnlock()

	return sw.toFetch.Len()
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

// IsWanted indicates if the session is expecting to receive the block with the
// given CID
func (sw *sessionWants) IsWanted(c cid.Cid) bool {
	sw.RLock()
	defer sw.RUnlock()

	return sw.unlockedIsWanted(c)
}

// FilterWanted filters the list so that it only contains keys for
// blocks that the session is expecting to receive
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

// CountWanted counts the number of keys in the list that correspond to
// blocks that the session is expecting to receive
func (sw *sessionWants) CountWanted(ks []cid.Cid) int {
	sw.RLock()
	defer sw.RUnlock()

	count := 0
	for _, k := range ks {
		if sw.unlockedIsWanted(k) {
			count++
		}
	}

	return count
}

// FilterInteresting filters the list so that it only contains keys for
// blocks that the session is waiting to receive or has received in the past
func (sw *sessionWants) FilterInteresting(ks []cid.Cid) []cid.Cid {
	sw.RLock()
	defer sw.RUnlock()

	var interested []cid.Cid
	for _, k := range ks {
		if sw.unlockedIsWanted(k) || sw.pastWants.Has(k) {
			interested = append(interested, k)
		}
	}

	return interested
}

func (sw *sessionWants) unlockedIsWanted(c cid.Cid) bool {
	_, ok := sw.liveWants[c]
	if !ok {
		ok = sw.toFetch.Has(c)
	}
	return ok
}
