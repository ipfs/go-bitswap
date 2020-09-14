package peermanager

import (
	"bytes"
	"fmt"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Gauge can be used to keep track of a metric that increases and decreases
// incrementally. It is used by the peerWantManager to track the number of
// want-blocks that are active (ie sent but no response received)
type Gauge interface {
	Inc()
	Dec()
}

// Indicates if a want is want-block or want-have
type wantType bool

const (
	wantTypeHave  wantType = false
	wantTypeBlock wantType = true
)

// The sessions that are interested in a want, and the type of the want
type wantInfo struct {
	sessions map[uint64]struct{}
	tp       wantType
}

// peerWantManager keeps track of which want-haves and want-blocks have been
// sent to each peer, so that the PeerManager doesn't send duplicates.
type peerWantManager struct {
	// peerWants maps peers to outstanding wants.
	// A peer's wants is the _union_ of the broadcast wants and the wants in
	// this list.
	peerWants map[peer.ID]*peerWant

	// Reverse index of all wants in peerWants.
	wantPeers map[cid.Cid]map[peer.ID]struct{}

	// broadcastWants tracks all the current broadcast wants.
	broadcastWants cidSessSet

	// Keeps track of the number of active want-haves & want-blocks
	wantGauge Gauge
	// Keeps track of the number of active want-blocks
	wantBlockGauge Gauge
}

// The wants sent to a particular peer
type peerWant struct {
	wants     map[cid.Cid]*wantInfo
	peerQueue PeerQueue
}

// New creates a new peerWantManager with a Gauge that keeps track of the
// number of active want-blocks (ie sent but no response received)
func newPeerWantManager(wantGauge Gauge, wantBlockGauge Gauge) *peerWantManager {
	return &peerWantManager{
		broadcastWants: newCidSessSet(),
		peerWants:      make(map[peer.ID]*peerWant),
		wantPeers:      make(map[cid.Cid]map[peer.ID]struct{}),
		wantGauge:      wantGauge,
		wantBlockGauge: wantBlockGauge,
	}
}

// addPeer adds a peer whose wants we need to keep track of. It sends the
// current list of broadcast wants to the peer.
func (pwm *peerWantManager) addPeer(peerQueue PeerQueue, p peer.ID) {
	if _, ok := pwm.peerWants[p]; ok {
		return
	}

	pwm.peerWants[p] = &peerWant{
		wants:     make(map[cid.Cid]*wantInfo),
		peerQueue: peerQueue,
	}

	// Broadcast any live want-haves to the newly connected peer
	if pwm.broadcastWants.len() > 0 {
		wants := pwm.broadcastWants.keys()
		peerQueue.AddBroadcastWantHaves(wants)
	}
}

// RemovePeer removes a peer and its associated wants from tracking
func (pwm *peerWantManager) removePeer(p peer.ID) {
	pws, ok := pwm.peerWants[p]
	if !ok {
		return
	}

	for c := range pws.wants {
		peerCntsBefore := pwm.wantPeerCounts(c)

		// Clean up wants from the reverse index
		pwm.reverseIndexRemove(c, p)

		// Decrement the want gauges
		peerCntsAfter := pwm.wantPeerCounts(c)
		pwm.decrementWantGauges(peerCntsBefore, peerCntsAfter)
	}

	delete(pwm.peerWants, p)
}

// broadcastWantHaves sends want-haves to any peers that have not yet been sent them.
func (pwm *peerWantManager) broadcastWantHaves(sid uint64, wantHaves []cid.Cid) {
	unsent := make([]cid.Cid, 0, len(wantHaves))
	for _, c := range wantHaves {
		// Add the broadcast want for this session
		isNew := pwm.broadcastWants.add(c, sid)

		// If this want hadn't already been sent, broadcast it
		if isNew {
			unsent = append(unsent, c)
		}

		// If no peer has a pending want for the key
		if _, ok := pwm.wantPeers[c]; !ok {
			// Increment the total wants gauge
			pwm.wantGauge.Inc()
		}
	}

	if len(unsent) == 0 {
		return
	}

	// Allocate a single buffer to filter broadcast wants for each peer
	bcstWantsBuffer := make([]cid.Cid, 0, len(unsent))

	// Send broadcast wants to each peer
	for _, pws := range pwm.peerWants {
		peerUnsent := bcstWantsBuffer[:0]
		for _, c := range unsent {
			// If we've already sent a want to this peer, skip them.
			if _, ok := pws.wants[c]; !ok {
				peerUnsent = append(peerUnsent, c)
			}
		}

		if len(peerUnsent) > 0 {
			pws.peerQueue.AddBroadcastWantHaves(peerUnsent)
		}
	}
}

// sendWants only sends the peer the want-blocks and want-haves that have not
// already been sent to it.
func (pwm *peerWantManager) sendWants(sid uint64, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	// Get the existing want-blocks and want-haves for the peer
	pws, ok := pwm.peerWants[p]
	if !ok {
		// In practice this should never happen
		log.Errorf("sendWants() called with peer %s but peer not found in peerWantManager", string(p))
		return
	}

	// Iterate over the requested want-blocks
	fltWantBlks := make([]cid.Cid, 0, len(wantBlocks))
	for _, c := range wantBlocks {
		wi, exists := pws.wants[c]
		sendWantBlock := !exists
		peerCntsBefore := pwm.wantPeerCounts(c)

		// Record that the CID was sent to the peer as a want-block:
		// If a want for this CID has already been sent to the peer
		if exists {
			// Add this session to the sessions who've sent the want
			wi.sessions[sid] = struct{}{}

			// If it was a want-have
			if wi.tp == wantTypeHave {
				// Change it to be a want-block
				wi.tp = wantTypeBlock
				// Be sure to send want-block
				sendWantBlock = true
			}
		} else {
			// A want has not been sent to the peer for this CID, so create wantInfo
			pws.wants[c] = &wantInfo{
				tp: wantTypeBlock,
				sessions: map[uint64]struct{}{
					sid: struct{}{},
				},
			}
		}

		// Check if a want-block for the CID has already been sent to the peer
		if !sendWantBlock {
			continue
		}

		// Increment the want gauges
		if peerCntsBefore.wantBlock == 0 {
			pwm.wantBlockGauge.Inc()
		}
		if !peerCntsBefore.wanted() {
			pwm.wantGauge.Inc()
		}

		// Update the reverse index
		pwm.reverseIndexAdd(c, p)

		// Add the CID to the results
		fltWantBlks = append(fltWantBlks, c)
	}

	// Iterate over the requested want-haves
	fltWantHvs := make([]cid.Cid, 0, len(wantHaves))
	for _, c := range wantHaves {
		wi, exists := pws.wants[c]
		peerCntsBefore := pwm.wantPeerCounts(c)

		// Record that the CID was sent to the peer as a want-have:
		// If a want for this CID has already been sent to the peer
		if exists {
			// Add this session to the sessions who've sent the want to the peer
			wi.sessions[sid] = struct{}{}
		} else {
			// A want has not been sent to the peer for this CID, so create wantInfo
			pws.wants[c] = &wantInfo{
				tp: wantTypeHave,
				sessions: map[uint64]struct{}{
					sid: struct{}{},
				},
			}
		}

		// Check if a want for the CID was already sent to the peer
		if exists {
			continue
		}

		// Update the reverse index
		pwm.reverseIndexAdd(c, p)

		// If the CID has not been sent as a want-block, want-have or broadcast
		if !peerCntsBefore.wanted() {
			// Increment the want gauge
			pwm.wantGauge.Inc()
		}

		// If we've already broadcasted this want, don't send another want-have.
		if peerCntsBefore.isBroadcast {
			continue
		}

		// Add the CID to the results
		fltWantHvs = append(fltWantHvs, c)
	}

	// Send the want-blocks and want-haves to the peer
	pws.peerQueue.AddWants(fltWantBlks, fltWantHvs)
}

// Used internally by the sendCancels to keep track of the state of a key
type bcstType int

const (
	// key is not a broadcast key
	bcstTypeNone bcstType = iota
	// there is at least one session that still wants the key
	bcstTypeInUse
	// no session wants the key
	bcstTypeCancellable
)

// sendCancels sends a cancel to each peer to which a corresponding want was
// sent. It will only send a cancel for keys that no session wants anymore.
func (pwm *peerWantManager) sendCancels(sid uint64, cancelKs []cid.Cid) {
	if len(cancelKs) == 0 {
		return
	}

	// Record how many peers have a pending want-block and want-have for each
	// key to be cancelled
	peerCounts := make([]wantPeerCnts, 0, len(cancelKs))
	for _, c := range cancelKs {
		peerCounts = append(peerCounts, pwm.wantPeerCounts(c))
	}

	// Find which broadcast wants are ready to be removed
	hasBcstWants := false
	bcstWants := make([]bcstType, len(cancelKs))
	for i, c := range cancelKs {
		// Remove the broadcast want
		cancellable, isBcstWant := pwm.broadcastWants.remove(c, sid)

		// Check if the cancel is for a broadcast want
		if !isBcstWant {
			bcstWants[i] = bcstTypeNone
			continue
		}

		hasBcstWants = true

		// If this was the last session that wanted the broadcast want, it's
		// ready to be removed
		if cancellable {
			bcstWants[i] = bcstTypeCancellable
		} else {
			bcstWants[i] = bcstTypeInUse
		}
	}

	// Allocate a single buffer to filter cancels per peer
	cancelBuffer := make([]cid.Cid, 0, len(cancelKs))

	// Send cancels to a particular peer
	send := func(p peer.ID, pws *peerWant) {
		toCancel := cancelBuffer[:0]

		// For each cancel
		for i, c := range cancelKs {
			peerWantCancellable := false

			// Get the wantInfo for the key
			wi, isPeerWant := pws.wants[c]

			// If the key is wanted by this peer
			if isPeerWant {
				// If the want was from this session
				if _, ok := wi.sessions[sid]; ok {
					// Remove the session
					delete(wi.sessions, sid)

					// If this was the last session that wanted the key
					if len(wi.sessions) == 0 {
						// Send a cancel
						peerWantCancellable = true

						// Clean up the reverse index
						pwm.reverseIndexRemove(c, p)

						// Clean up the want info for the key
						delete(pws.wants, c)
					}
				}
			}

			// Cancel the want if this session was the last session that wanted
			// it in either the broadcast list or the peer want list, and there
			// are no other sessions that want it in either of those lists.
			bcstTp := bcstWants[i]
			bcstStillWants := bcstTp == bcstTypeInUse
			bcstWantCancellable := bcstTp == bcstTypeCancellable

			peerStillWants := isPeerWant && !peerWantCancellable
			if (bcstWantCancellable || peerWantCancellable) && !peerStillWants && !bcstStillWants {
				toCancel = append(toCancel, c)
			}
		}

		// Send cancels to the peer
		if len(toCancel) > 0 {
			pws.peerQueue.AddCancels(toCancel)
		}
	}

	if hasBcstWants {
		// If a broadcast want is being cancelled, send the cancel to all
		// peers
		for p, pws := range pwm.peerWants {
			send(p, pws)
		}
	} else {
		// Only send cancels to peers that received a corresponding want
		cancelPeers := make(map[peer.ID]struct{}, len(pwm.wantPeers[cancelKs[0]]))
		for _, c := range cancelKs {
			for p := range pwm.wantPeers[c] {
				cancelPeers[p] = struct{}{}
			}
		}
		for p := range cancelPeers {
			pws, ok := pwm.peerWants[p]
			if !ok {
				// Should never happen but check just in case
				log.Errorf("sendCancels - peerWantManager index missing peer %s", p)
				continue
			}

			send(p, pws)
		}
	}

	// Decrement the want gauges
	for i, c := range cancelKs {
		peerCntsBefore := peerCounts[i]
		peerCntsAfter := pwm.wantPeerCounts(c)
		pwm.decrementWantGauges(peerCntsBefore, peerCntsAfter)
	}
}

// unwanted filters the keys for those that are no longer wanted by any session
func (pwm *peerWantManager) unwanted(ks []cid.Cid) []cid.Cid {
	unw := make([]cid.Cid, 0, len(ks))
	for _, c := range ks {
		if pwm.broadcastWants.has(c) {
			continue
		}

		if _, ok := pwm.wantPeers[c]; ok {
			continue
		}

		unw = append(unw, c)
	}

	return unw
}

// wantPeerCnts stores the number of peers that have pending wants for a CID
type wantPeerCnts struct {
	// number of peers that have a pending want-block for the CID
	wantBlock int
	// number of peers that have a pending want-have for the CID
	wantHave int
	// whether the CID is a broadcast want
	isBroadcast bool
}

// wanted returns true if any peer wants the CID or it's a broadcast want
func (pwm *wantPeerCnts) wanted() bool {
	return pwm.wantBlock > 0 || pwm.wantHave > 0 || pwm.isBroadcast
}

// wantPeerCounts counts how many peers have a pending want-block and want-have
// for the given CID
func (pwm *peerWantManager) wantPeerCounts(c cid.Cid) wantPeerCnts {
	blockCount := 0
	haveCount := 0
	for p := range pwm.wantPeers[c] {
		pws, ok := pwm.peerWants[p]
		if !ok {
			log.Errorf("reverse index has extra peer %s for key %s in peerWantManager", string(p), c)
			continue
		}

		wi, ok := pws.wants[c]
		if !ok {
			log.Errorf("index missing key %s for peer %s in reverse index in peerWantManager", c, string(p))
			continue
		}

		if wi.tp == wantTypeBlock {
			blockCount++
		} else {
			haveCount++
		}
	}

	return wantPeerCnts{blockCount, haveCount, pwm.broadcastWants.has(c)}
}

func (pwm *peerWantManager) decrementWantGauges(before wantPeerCnts, after wantPeerCnts) {
	// If there were any peers that had a pending want-block for the key
	// that has now been cancelled
	if before.wantBlock > 0 && after.wantBlock == 0 {
		// Decrement the want-block gauge
		pwm.wantBlockGauge.Dec()
	}

	// If there was a peer that had a pending want or it was a broadcast want
	// and now it is no longer wanted
	if before.wanted() && !after.wanted() {
		// Decrement the total wants gauge
		pwm.wantGauge.Dec()
	}
}

// Add the peer to the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexAdd(c cid.Cid, p peer.ID) {
	peers, ok := pwm.wantPeers[c]
	if !ok {
		peers = make(map[peer.ID]struct{}, 10)
		pwm.wantPeers[c] = peers
	}
	peers[p] = struct{}{}
}

// Remove the peer from the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexRemove(c cid.Cid, p peer.ID) {
	if peers, ok := pwm.wantPeers[c]; ok {
		delete(peers, p)
		if len(peers) == 0 {
			delete(pwm.wantPeers, c)
		}
	}
}

// GetWantBlocks returns the set of all want-blocks sent to all peers
func (pwm *peerWantManager) getWantBlocks() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-blocks
		for c, wi := range pws.wants {
			// Add the CID to the results
			if wi.tp == wantTypeBlock {
				res.Add(c)
			}
		}
	}

	return res.Keys()
}

// GetWantHaves returns the set of all want-haves sent to all peers
func (pwm *peerWantManager) getWantHaves() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all peers with active wants.
	for _, pws := range pwm.peerWants {
		// Iterate over all want-haves
		for c, wi := range pws.wants {
			// Add the CID to the results
			if wi.tp == wantTypeHave {
				res.Add(c)
			}
		}
	}

	// Add broadcast want-haves
	for c := range pwm.broadcastWants {
		res.Add(c)
	}

	return res.Keys()
}

// GetWants returns the set of all wants (both want-blocks and want-haves).
func (pwm *peerWantManager) getWants() []cid.Cid {
	res := pwm.broadcastWants.keys()

	// Iterate over all targeted wants, removing ones that are also in the
	// broadcast list.
	for c := range pwm.wantPeers {
		if !pwm.broadcastWants.has(c) {
			res = append(res, c)
		}
	}

	return res
}

func (pwm *peerWantManager) String() string {
	var b bytes.Buffer
	for p, ws := range pwm.peerWants {
		b.WriteString(fmt.Sprintf("Peer %s:\n", p))
		for c, wi := range ws.wants {
			tp := "want-have"
			if wi.tp == wantTypeBlock {
				tp = "want-block"
			}
			b.WriteString(fmt.Sprintf("  %s  %s\n", tp, c))
		}
	}
	return b.String()
}

// cidSessSet keeps track of which keys are wanted by which sessions
type cidSessSet map[cid.Cid]map[uint64]struct{}

func newCidSessSet() cidSessSet {
	return make(cidSessSet)
}

// has indicates whether any session wants the key
func (css cidSessSet) has(c cid.Cid) bool {
	_, ok := css[c]
	return ok
}

// keys returns an array of all keys
func (css cidSessSet) keys() []cid.Cid {
	ks := make([]cid.Cid, 0, len(css))
	for c := range css {
		ks = append(ks, c)
	}
	return ks
}

// add a session that wants the key
// returns true if this is the only session that wants the key
func (css cidSessSet) add(c cid.Cid, sid uint64) bool {
	s, ok := css[c]
	if !ok {
		css[c] = map[uint64]struct{}{sid: struct{}{}}
		return true
	}
	s[sid] = struct{}{}
	return false
}

// remove the session's interest in the key
// returns
// - true if this was the last session that was interested in the key
// - true if there was a session interested in the key
func (css cidSessSet) remove(c cid.Cid, sid uint64) (bool, bool) {
	s, ok := css[c]
	if !ok {
		return false, false
	}

	delete(s, sid)
	if len(s) == 0 {
		delete(css, c)
	}
	return len(s) == 0, true
}

// len returns the number of keys
func (css cidSessSet) len() int {
	return len(css)
}
