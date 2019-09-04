package session

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Amount to increase potential by if we received a HAVE message
const rcvdHavePotentialGain = 0.8

type sessionWants struct {
	sync.RWMutex
	toFetch   *cidQueue
	// liveWants map[cid.Cid]time.Time
	liveWants liveWantsInfo
	pastWants *cid.Set

	// wantPotential      map[cid.Cid]map[peer.ID]float64
	// wantPeers          map[cid.Cid][]peer.ID
	potentialThreshold float64

	// blockPresence map[cid.Cid]map[peer.ID]bool
	maxPeerHaves int
}

func newSessionWants() sessionWants {
	return sessionWants{
		// liveWants:          make(map[cid.Cid]time.Time),
		liveWants:          newLiveWantsInfo(),
		toFetch:            newCidQueue(),
		pastWants:          cid.NewSet(),
		// wantPotential:      make(map[cid.Cid]map[peer.ID]float64),
		// wantPeers:          make(map[cid.Cid][]peer.ID),
		// blockPresence:      make(map[cid.Cid]map[peer.ID]bool),
		potentialThreshold: 1.5,
		maxPeerHaves:       16,
	}
}

func (sw *sessionWants) String() string {
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
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	totalLatency := time.Duration(0)
	wanted := make([]cid.Cid, 0, len(cids))
	for _, c := range cids {
		if sw.unlockedIsWanted(c) {
			wanted = append(wanted, c)

			li, ok := sw.liveWants[c]
			if ok && !li.sentAt.IsZero() {
				totalLatency += now.Sub(li.sentAt)
			}

			// Remove the CID from the live wants / toFetch queue and add it
			// to the past wants
			sw.liveWants.clearLiveWant(c)
			sw.toFetch.Remove(c)
			sw.pastWants.Add(c)
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
		sw.liveWants.setSentAt(c, now)
	}

	return live
}

// PrepareBroadcast saves the current time for each live want and returns the
// live want CIDs.
func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	// for _, c := range sw.toFetch.Cids() {
	// 	live = append(live, c)
	// 	sw.liveWants.setSentAt(c, now)
	// }
	for c := range sw.liveWants {
		live = append(live, c)
		sw.liveWants.setSentAt(c, now)
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

	// For each HAVE
	for _, c := range haves {
		// log.Warningf("      sesswants: info<-%s HAVE: %s (potential %.2f)\n", p, c.String()[2:8], sw.sumWantPotential(c))
		// If we want this block, record that the peer has the block
		if sw.unlockedIsWanted(c) {
			sw.liveWants.updateBlockPresence(c, p, true)
			// log.Warningf("        updated block presence to HAVE\n")
		}
	}

	// For each DONT_HAVE
	for _, c := range dontHaves {
		// log.Warningf("      sesswants: info<-%s DONT_HAVE: %s (potential %.2f)\n", p, c.String()[2:8], sw.sumWantPotential(c))
		// If we sent a want-block to this peer, decrease the want's potential by the
		// corresponding amount
		if sw.liveWants.wasWantBlockSentToPeer(c, p) {
			sw.liveWants.removeWantPotential(c, p)
			// log.Warningf("        new potential:  %.2f\n", sw.sumWantPotential(c))
		}
		// If we want this block, record that the peer does not have the block
		if sw.unlockedIsWanted(c) {
			sw.liveWants.updateBlockPresence(c, p, false)
			// log.Warningf("        updated block presence to DONT_HAVE\n")
		}
	}
}

func (sw *sessionWants) PopNextPending(peers []peer.ID) (cid.Cid, []cid.Cid, peer.ID, []peer.ID) {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	var wantHaves []cid.Cid
log.Warningf("sw %s", sw)

	c, p, potential, ph := sw.getBestPotentialLiveWant(peers)
	if c.Defined() {
		sw.liveWants.setWantBlockSentToPeer(c, p)
		sw.liveWants.addWantPotential(c, p, potential)
		if _, ok := sw.liveWants[c]; !ok {
			sw.toFetch.Remove(c)
			sw.liveWants.setSentAt(c, now)
		}
		wantHaves = sw.getPiggybackWantHaves(c, p)
		for _, whp := range wantHaves {
			sw.liveWants.setWantHaveSentToPeer(whp, p)
		}
		for _, php := range ph {
			sw.liveWants.setWantHaveSentToPeer(c, php)
		}
	}
log.Warningf("sw %s", sw)

	return c, wantHaves, p, ph
}

// When we send a want-block to a peer, we also include want-haves for other
// wants in our fetch / live want lists in the request
func (sw *sessionWants) getPiggybackWantHaves(c cid.Cid, p peer.ID) []cid.Cid {
	others := make([]cid.Cid, 0)

	// Include other cids from the fetch queue
	for _, k := range sw.toFetch.Cids() {
		if !c.Equals(k) && !sw.liveWants.wasWantBlockSentToPeer(k, p) && !sw.liveWants.wasWantHaveSentToPeer(k, p) {
			others = append(others, k)
		}
	}
	// Include other cids from the live queue
	for k := range sw.liveWants {
		if !c.Equals(k) && !sw.liveWants.wasWantBlockSentToPeer(k, p) && !sw.liveWants.wasWantHaveSentToPeer(k, p) {
			others = append(others, k)
		}
	}

	return others
}

type potentialGain struct {
	cid       cid.Cid
	potential float64
	maxGain   float64
	peer      peer.ID
}

func (pg potentialGain) String() string {
	return fmt.Sprintf("%s potential %f. Gain for %s: %f", pg.cid.String()[2:8], pg.potential, pg.peer, pg.maxGain)
}

func potentialGainLess(pgs []potentialGain) func(i, j int) bool {
	indices := make(map[cid.Cid]int, len(pgs))
	for i, pg := range pgs {
		indices[pg.cid] = i
	}

	return func(i, j int) bool {
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

func (sw *sessionWants) getBestPotentialLiveWant(peers []peer.ID) (cid.Cid, peer.ID, float64, []peer.ID) {
	// fmt.Printf("getBestPotentialLiveWant(%s)\n", peers)
	bestC := cid.Cid{}
	bestP := peer.ID("")
	bestPotential := -1.0
	peerHaves := make([]peer.ID, 0)

	// Work out the best peer to send each want to, and how big a potential
	// would be gained
	pgs := make([]potentialGain, 0, sw.toFetch.Len()+len(sw.liveWants))
	for c := range sw.liveWants {
		// Check if the want already already has enough potential
		potential := sw.liveWants.sumWantPotential(c)
		if potential < sw.potentialThreshold {
			// log.Debugf("%s: live want potential %.2f is below threshold %.2f", c.String()[2:6], potential, sw.potentialThreshold)
			maxPeer, maxGain := sw.getPotentialGain(c, peers)
			if maxPeer != "" {
				pgs = append(pgs, potentialGain{
					cid: c,
					potential: potential,
					maxGain: maxGain,
					peer: maxPeer,
				})
			}
		} else {
			// log.Debugf("%s: live want potential %.2f is above threshold %.2f", c.String()[2:6], potential, sw.potentialThreshold)
		}
	}
	for _, c := range sw.toFetch.Cids() {
		// log.Debugf("%s: to fetch want selected (threshold %.2f)", c.String()[2:6], sw.potentialThreshold)
		// fmt.Printf("  selecting %s from fetch queue (session threshold %f)\n", c.String()[2:8], sw.potentialThreshold)
		maxPeer, maxGain := sw.getPotentialGain(c, peers)
		if maxPeer != "" {
			pgs = append(pgs, potentialGain{
				cid: c,
				potential: sw.liveWants.sumWantPotential(c),
				maxGain: maxGain,
				peer: maxPeer,
			})
		}
	}

	if len(pgs) == 0 {
		// totalUnder := 0
		// totalWithUncontactedPeers := 0
		// for c, _ := range sw.liveWants {
		// 	potential := sw.sumWantPotential(c)
		// 	if potential < sw.potentialThreshold {
		// 		totalUnder++
		// 		candidates := sw.wantBlockNotSentToPeers(c, peers)
		// 		if len(candidates) > 0 {
		// 			totalWithUncontactedPeers++
		// 		}
		// 	}
		// }
		// log.Debugf("     No match of %d peers - threshold: %.2f, fetch: %d, live: %d, past: %d", len(peers), sw.potentialThreshold, sw.toFetch.Len(), len(sw.liveWants), sw.pastWants.Len())
		// log.Debugf("       Total under threshold: %d. Total with uncontacted peers: %d", totalUnder, totalWithUncontactedPeers)
		return bestC, bestP, bestPotential, peerHaves
	}

	sort.Slice(pgs, potentialGainLess(pgs))
	bestPg := pgs[0]
	if bestPg.maxGain >= 0 {
		bestC, bestP, bestPotential = bestPg.cid, bestPg.peer, bestPg.maxGain

		// peerHavesCnt := 0
		// for i := 1; i < len(pgs) && peerHavesCnt < sw.maxPeerHaves; i++ {
		// 	if pgs[i].peer != bestP && pgs[i].maxGain >= 0 {
		// 		peerHaves = append(peerHaves, pgs[i].peer)
		// 		peerHavesCnt++
		// 	}
		// }

		// Send a want-have to each other candidate peer up to maxPeerHaves
		peerHavesCnt := 0
		for i := 0; i < len(peers) && peerHavesCnt < sw.maxPeerHaves; i++ {
			if peers[i] != bestP {
				// Don't bother sending want-have if the peer already sent us
				// DONT_HAVE for this CID
				if !sw.liveWants.receivedDontHaveFromPeer(bestC, peers[i]) {
					peerHaves = append(peerHaves, peers[i])
					peerHavesCnt++
				}
			}
		}
	}
	log.Debugf("Best: %s (%d candidates)\n", bestPg, len(pgs))
	return bestC, bestP, bestPotential, peerHaves
}

// Out of the given peers, get the peer with the highest potential gain for the
// given cid
func (sw *sessionWants) getPotentialGain(c cid.Cid, peers []peer.ID) (peer.ID, float64) {
	var maxPeer peer.ID
	maxGain := -1.0

	// Filter peers for those that we haven't yet sent a want-block to for this CID
	candidates := sw.liveWants.wantBlockNotSentToPeers(c, peers)
	for _, p := range candidates {
		gain := 0.0

		// If the peer sent us a HAVE or HAVE_NOT for the cid, adjust the
		// potential for the peer / cid combination
		if sw.liveWants.receivedHaveFromPeer(c, p) {
			gain += rcvdHavePotentialGain
		} else if sw.liveWants.receivedDontHaveFromPeer(c, p) {
			gain -= rcvdHavePotentialGain
		} else {
			// TODO: Take into account peer's uniq / dup ratio
			gain += 0.5
		}

		// TODO: Take into account peer's uniq / dup ratio
		// For now choose at random between peers with equal gain
		betterPeer := rand.Intn(2) == 0

		if gain >= 0 && (gain > maxGain || (gain == maxGain && betterPeer)) {
			maxPeer = p
			maxGain = gain
		}
	}

	// // Current total potential for the cid
	// potential := sw.sumWantPotential(c)
	// return potentialGain{c, potential, maxGain, maxPeer}
	return maxPeer, maxGain
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

type liveInfo struct {
	peerPotential map[peer.ID]float64
	blockPresence map[peer.ID]bool
	sentWantBlockTo []peer.ID
	sentWantHaveTo []peer.ID
	sentAt time.Time
}

type liveWantsInfo map[cid.Cid]*liveInfo

func newLiveWantsInfo() liveWantsInfo {
	return make(liveWantsInfo)
}

func (wi liveWantsInfo) clearLiveWant(c cid.Cid) {
	delete(wi, c)
}

func (wi liveWantsInfo) getOrCreate(c cid.Cid) *liveInfo {
	_, ok := wi[c]
	if !ok {
		wi[c] = &liveInfo{
			peerPotential: make(map[peer.ID]float64),
			blockPresence: make(map[peer.ID]bool),
			sentWantBlockTo: make([]peer.ID, 0),
			sentWantHaveTo: make([]peer.ID, 0),
			sentAt: time.Time{},
		}
	}
	return wi[c]
}

func (wi liveWantsInfo) setSentAt(c cid.Cid, at time.Time) {
	li := wi.getOrCreate(c)
	li.sentAt = at
	wi[c] = li
}

func (wi liveWantsInfo) addWantPotential(c cid.Cid, p peer.ID, gain float64) {
	li := wi.getOrCreate(c)
	li.peerPotential[p] = gain
}

func (wi liveWantsInfo) removeWantPotential(c cid.Cid, p peer.ID) {
	if li, ok := wi[c]; ok {
		delete(li.peerPotential, p)
	}
}

func (wi liveWantsInfo) sumWantPotential(c cid.Cid) float64 {
	total := 0.0
	if li, ok := wi[c]; ok {
		for _, amount := range li.peerPotential {
			total += amount
		}
	}
	return total
}

func (wi liveWantsInfo) updateBlockPresence(c cid.Cid, p peer.ID, present bool) {
	li := wi.getOrCreate(c)
	has, ok := li.blockPresence[p]
	// Make sure not to change HAVE to DONT_HAVE
	if ok && has {
		return
	}
	li.blockPresence[p] = present
}

func (wi liveWantsInfo) receivedHaveFromPeer(c cid.Cid, p peer.ID) bool {
	if li, ok := wi[c]; ok {
		if has, pok := li.blockPresence[p]; pok && has {
			return true
		}
	}
	return false
}

func (wi liveWantsInfo) receivedDontHaveFromPeer(c cid.Cid, p peer.ID) bool {
	if li, ok := wi[c]; ok {
		if has, pok := li.blockPresence[p]; pok && !has {
			return true
		}
	}
	return false
}

func (wi liveWantsInfo) wasWantBlockSentToPeer(c cid.Cid, p peer.ID) bool {
	li, ok := wi[c]
	if !ok {
		return false
	}
	for _, stp := range li.sentWantBlockTo {
		if stp == p {
			return true
		}
	}
	return false
}

func (wi liveWantsInfo) wasWantHaveSentToPeer(c cid.Cid, p peer.ID) bool {
	li, ok := wi[c]
	if !ok {
		return false
	}
	for _, stp := range li.sentWantHaveTo {
		if stp == p {
			return true
		}
	}
	return false
}

func (wi liveWantsInfo) wantBlockNotSentToPeers(c cid.Cid, peers []peer.ID) []peer.ID {
	li, ok := wi[c]
	if !ok {
		// fmt.Printf("      wantBlockNotSentToPeers no record\n")
		return peers
	}

	ps := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		sentWantBlockToPeer := false
		for _, stp := range li.sentWantBlockTo {
			if p == stp {
				sentWantBlockToPeer = true
			}
		}
		if !sentWantBlockToPeer {
			ps = append(ps, p)
		}
	}
	return ps
}

func (wi liveWantsInfo) setWantBlockSentToPeer(c cid.Cid, p peer.ID) {
	li := wi.getOrCreate(c)
	li.sentWantBlockTo = append(li.sentWantBlockTo, p)
}

func (wi liveWantsInfo) setWantHaveSentToPeer(c cid.Cid, p peer.ID) {
	li := wi.getOrCreate(c)
	li.sentWantHaveTo = append(li.sentWantHaveTo, p)
}
