package session

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Amount to increase potential by if we received a HAVE message
const rcvdHavePotentialGain = 0.8

type BlockSentManager interface {
	PeersCanSendWantBlock(c cid.Cid, peers []peer.ID) []peer.ID
	PeerCanSendWants(p peer.ID, wants []cid.Cid) []cid.Cid
}

type sessionWants struct {
	sync.RWMutex
	toFetch   *cidQueue
	liveWants liveWantsInfo
	pastWants *cid.Set

	bpm                   *bsbpm.BlockPresenceManager
	bsm                   BlockSentManager
	potentialThresholdMgr *potentialThresholdManager

	maxPeerHaves int
}

func newSessionWants(bpm *bsbpm.BlockPresenceManager, bsm BlockSentManager) sessionWants {
	return sessionWants{
		liveWants:             newLiveWantsInfo(),
		toFetch:               newCidQueue(),
		pastWants:             cid.NewSet(),
		bpm:                   bpm,
		bsm:                   bsm,
		potentialThresholdMgr: newPotentialThresholdManager(),
		maxPeerHaves:          16,
	}
}

func (sw *sessionWants) String() string {
	return fmt.Sprintf("%d past / %d pending / %d live", sw.pastWants.Len(), sw.toFetch.Len(), len(sw.liveWants))
}

// ReceiveFrom moves received block CIDs from live to past wants and
// measures latency. It returns the CIDs of blocks that were actually
// wanted (as opposed to duplicates) and the total latency for all incoming blocks.
func (sw *sessionWants) ReceiveFrom(p peer.ID, ks []cid.Cid, dontHaves []cid.Cid) ([]cid.Cid, time.Duration) {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	// Only record uniqs / dups if the block came from the network
	// (as opposed to coming from the local node)
	if p != "" {
		uniqKs, dupKs := sw.uniqDups(ks)
		sw.potentialThresholdMgr.ReceivedBlocks(uniqKs, dupKs)
	}

	sw.dontHavesReceived(p, dontHaves)

	totalLatency := time.Duration(0)
	wanted := make([]cid.Cid, 0, len(ks))
	for _, c := range ks {
		if sw.unlockedIsWanted(c) {
			wanted = append(wanted, c)

			li, ok := sw.liveWants[c]
			if ok && !li.sentAt.IsZero() {
				totalLatency += now.Sub(li.sentAt)
			}

			// Remove the CID from the live wants / toFetch queue and add it
			// to the past wants
			sw.liveWants.removeLiveWant(c)
			sw.toFetch.Remove(c)
			sw.pastWants.Add(c)
		}
	}

	return wanted, totalLatency
}

func (sw *sessionWants) dontHavesReceived(p peer.ID, dontHaves []cid.Cid) {
	// For each DONT_HAVE
	for _, c := range dontHaves {
		// If we sent a want-block to this peer, decrease the want's potential by the
		// corresponding amount
		sw.liveWants.removeWantPotential(c, p)
	}
}

func (sw *sessionWants) uniqDups(ks []cid.Cid) ([]cid.Cid, []cid.Cid) {
	uniqs := make([]cid.Cid, 0)
	dups := make([]cid.Cid, 0)
	for _, k := range ks {
		if sw.unlockedIsWanted(k) {
			uniqs = append(uniqs, k)
		} else if sw.pastWants.Has(k) {
			dups = append(dups, k)
		}
	}
	return uniqs, dups
}

func (sw *sessionWants) IdleTimeout() {
	sw.potentialThresholdMgr.IdleTimeout()
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

func (sw *sessionWants) BlocksRequested(newWants []cid.Cid) {
	sw.Lock()
	defer sw.Unlock()

	for _, k := range newWants {
		sw.toFetch.Push(k)
	}
}

func (sw *sessionWants) PopNextPending(peers []peer.ID) (cid.Cid, []cid.Cid, peer.ID, []peer.ID) {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	var wantHaves []cid.Cid

	c, p, potential, ph := sw.getBestPotentialLiveWant(peers)
	if c.Defined() {
		// log.Warningf("getBestPotentialLiveWant(%d peers): ->%s want-block %s (+%.2f: sum %.2f / threshold %.2f) %d peer-haves",
		// 	len(peers), p, c.String()[2:8], potential, sw.liveWants.sumWantPotential(c), sw.potentialThreshold, len(ph))
		if _, ok := sw.liveWants[c]; !ok {
			sw.toFetch.Remove(c)
			sw.liveWants.setSentAt(c, now)
		}
		sw.liveWants.addWantPotential(c, p, potential)
		wantHaves = sw.getPiggybackWantHaves(c, p)
	}

	return c, wantHaves, p, ph
}

// When we send a want-block to a peer, we also include want-haves for other
// wants in our fetch / live want lists in the request
func (sw *sessionWants) getPiggybackWantHaves(c cid.Cid, p peer.ID) []cid.Cid {
	toFetch := sw.toFetch.Cids()
	others := make([]cid.Cid, 0, len(toFetch)+len(sw.liveWants))

	// Include other cids from the fetch queue
	for _, k := range toFetch {
		// if !c.Equals(k) && !sw.liveWants.wasWantBlockSentToPeer(k, p) && !sw.liveWants.wasWantHaveSentToPeer(k, p) {
		if !c.Equals(k) {
			others = append(others, k)
		}
	}
	// Include other cids from the live queue
	for k := range sw.liveWants {
		// if !c.Equals(k) && !sw.liveWants.wasWantBlockSentToPeer(k, p) && !sw.liveWants.wasWantHaveSentToPeer(k, p) {
		if !c.Equals(k) {
			others = append(others, k)
		}
	}

	return sw.bsm.PeerCanSendWants(p, others)
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
		// Check if the want already has enough potential
		potential := sw.liveWants.sumWantPotential(c)
		if potential < sw.potentialThresholdMgr.PotentialThreshold() {
			// log.Warningf("  %s: live want potential %.2f is below threshold %.2f", c.String()[2:6], potential, sw.potentialThreshold)
			maxPeer, maxGain := sw.getPotentialGain(c, peers)
			if maxPeer != "" {
				pgs = append(pgs, potentialGain{
					cid:       c,
					potential: potential,
					maxGain:   maxGain,
					peer:      maxPeer,
				})
			}
		} else {
			// log.Warningf("  %s: live want potential %.2f is above threshold %.2f", c.String()[2:6], potential, sw.potentialThreshold)
		}
	}
	for _, c := range sw.toFetch.Cids() {
		// log.Warningf("  %s: to fetch want selected (threshold %.2f)", c.String()[2:6], sw.potentialThreshold)
		// fmt.Printf("  selecting %s from fetch queue (session threshold %f)\n", c.String()[2:8], sw.potentialThreshold)
		maxPeer, maxGain := sw.getPotentialGain(c, peers)
		if maxPeer != "" {
			pgs = append(pgs, potentialGain{
				cid:       c,
				potential: sw.liveWants.sumWantPotential(c),
				maxGain:   maxGain,
				peer:      maxPeer,
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
				if !sw.bpm.PeerDoesNotHaveBlock(peers[i], bestC) {
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
	candidates := sw.bsm.PeersCanSendWantBlock(c, peers)
	for _, p := range candidates {
		gain := 0.0

		// If the peer sent us a HAVE or HAVE_NOT for the cid, adjust the
		// potential for the peer / cid combination
		if sw.bpm.PeerHasBlock(p, c) {
			gain += rcvdHavePotentialGain
		} else if sw.bpm.PeerDoesNotHaveBlock(p, c) {
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
	sentAt        time.Time
}

type liveWantsInfo map[cid.Cid]*liveInfo

func newLiveWantsInfo() liveWantsInfo {
	return make(liveWantsInfo)
}

func (wi liveWantsInfo) removeLiveWant(c cid.Cid) {
	delete(wi, c)
}

func (wi liveWantsInfo) setSentAt(c cid.Cid, at time.Time) {
	_, ok := wi[c]
	if ok {
		wi[c].sentAt = at
	} else {
		wi[c] = &liveInfo{
			peerPotential: make(map[peer.ID]float64),
			sentAt:        time.Time{},
		}
	}
}

func (wi liveWantsInfo) addWantPotential(c cid.Cid, p peer.ID, gain float64) {
	li, ok := wi[c]
	if !ok {
		panic("addWantPotential() should only ever be called after setSentAt()")
	}
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
