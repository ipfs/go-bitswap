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

type wantType bool

const (
	wantTypeHave  wantType = false
	wantTypeBlock wantType = true
)

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

	for c, wi := range pws.wants {
		// Clean up wants from the reverse index
		removedLastPeer := pwm.reverseIndexRemove(c, p)
		if !removedLastPeer {
			continue
		}

		// Decrement the want gauges
		if wi.tp == wantTypeBlock {
			pwm.wantBlockGauge.Dec()
		}
		if !pwm.broadcastWants.has(c) {
			pwm.wantGauge.Dec()
		}
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

		// Increment the total wants gauge
		if _, ok := pwm.wantPeers[c]; !ok {
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
		return
	}

	// Iterate over the requested want-blocks
	fltWantBlks := make([]cid.Cid, 0, len(wantBlocks))
	for _, c := range wantBlocks {
		// Record that the CID was sent to the peer as a want-block
		wi, exists := pws.wants[c]
		sendWantBlock := !exists

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

		// Add the CID to the results
		fltWantBlks = append(fltWantBlks, c)

		// Update the reverse index
		peers := pwm.reverseIndexAdd(c, p)

		// If this is the only peer that sent a want for the CID
		// or if no other peers sent want-block (they only sent want-have),
		// increment the want-block gauge
		if len(peers) == 1 || !pwm.otherPeerSentWantBlock(c, peers, p) {
			pwm.wantBlockGauge.Inc()
		}

		// If this is the only peer that has sent a want for the CID
		// and the want hasn't already been counted as a want-have
		// and the want hasn't already been counted as a broadcast want,
		// increment the want gauge
		if len(peers) == 1 && !exists && !pwm.broadcastWants.has(c) {
			pwm.wantGauge.Inc()
		}
	}

	// Iterate over the requested want-haves
	fltWantHvs := make([]cid.Cid, 0, len(wantHaves))
	for _, c := range wantHaves {
		// Record that the CID was sent to the peer as a want-have
		wi, exists := pws.wants[c]

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

		// Check whether this is the first session that wants this block
		if exists {
			continue
		}

		// Update the reverse index
		peers := pwm.reverseIndexAdd(c, p)

		// If we've already broadcasted this want, don't send another want-have.
		if pwm.broadcastWants.has(c) {
			continue
		}

		// Add the CID to the results
		fltWantHvs = append(fltWantHvs, c)

		// Increment the total wants gauge
		if len(peers) == 1 {
			pwm.wantGauge.Inc()
		}
	}

	// Send the want-blocks and want-haves to the peer
	pws.peerQueue.AddWants(fltWantBlks, fltWantHvs)
}

// Check each other peer to see if they have sent a want-block
// for the CID (they may have sent a want-have)
func (pwm *peerWantManager) otherPeerSentWantBlock(c cid.Cid, peers map[peer.ID]struct{}, thisPeer peer.ID) bool {
	for p := range peers {
		if p == thisPeer {
			continue
		}

		pws, ok := pwm.peerWants[p]
		if !ok {
			continue
		}

		if wi, ok := pws.wants[c]; ok && wi.tp == wantTypeBlock {
			return true
		}
	}

	return false
}

// sendCancels sends a cancel to each peer to which a corresponding want was
// sent. It will only send a cancel for keys that no session wants anymore.
func (pwm *peerWantManager) sendCancels(sid uint64, cancelKs []cid.Cid) {
	if len(cancelKs) == 0 {
		return
	}

	// Find which broadcast wants are ready to be removed
	bcstWants := make(map[cid.Cid]bool, len(cancelKs))
	for _, c := range cancelKs {
		// Remove the broadcast want
		cancellable, isBcstWant := pwm.broadcastWants.remove(c, sid)

		// If the cancel is for a broadcast want
		if isBcstWant {
			// If this was the last session that wanted the broadcast want, it's
			// ready to be removed
			bcstWants[c] = cancellable
		}
	}

	// Allocate a single buffer to filter cancels per peer
	cancelBuffer := make([]cid.Cid, 0, len(cancelKs))

	cancelledWantBlocks := cid.NewSet()
	cancelledWantHaves := cid.NewSet()

	// Send cancels to a particular peer
	send := func(p peer.ID, pws *peerWant) {
		toCancel := cancelBuffer[:0]

		// For each cancel
		for _, c := range cancelKs {
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

						// Clean up the want info
						delete(pws.wants, c)
					}
				}
			}

			bcstWantCancellable, isBcstWant := bcstWants[c]

			// Cancel the want if this session was the last session that wanted
			// it in either the broadcast list or the peer want list, and there
			// are no other sessions that want it in either of those lists.
			peerStillWants := isPeerWant && !peerWantCancellable
			bcstStillWants := isBcstWant && !bcstWantCancellable
			if (bcstWantCancellable || peerWantCancellable) && !peerStillWants && !bcstStillWants {
				toCancel = append(toCancel, c)
			}

			// TODO: figure out this logic
			// Update the want gauges
			if peerWantCancellable {
				if wi.tp == wantTypeBlock {
					cancelledWantBlocks.Add(c)
				} else if !isBcstWant || bcstWantCancellable {
					cancelledWantHaves.Add(c)
				}
			} else if bcstWantCancellable {
				cancelledWantHaves.Add(c)
			}
		}

		// Send cancels to the peer
		if len(toCancel) > 0 {
			pws.peerQueue.AddCancels(toCancel)
		}
	}

	if len(bcstWants) > 0 {
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

	// Remove cancelled broadcast wants
	for c := range bcstWants {
		pwm.broadcastWants.remove(c, sid)

		// TODO: figure out this logic
		// Decrement the total wants gauge for broadcast wants
		if !cancelledWantHaves.Has(c) && !cancelledWantBlocks.Has(c) {
			pwm.wantGauge.Dec()
		}
	}

	// TODO: figure out this logic
	// Decrement the total wants gauge for peer wants
	_ = cancelledWantHaves.ForEach(func(c cid.Cid) error {
		pwm.wantGauge.Dec()
		return nil
	})
	_ = cancelledWantBlocks.ForEach(func(c cid.Cid) error {
		pwm.wantGauge.Dec()
		pwm.wantBlockGauge.Dec()
		return nil
	})
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

// Add the peer to the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexAdd(c cid.Cid, p peer.ID) map[peer.ID]struct{} {
	peers, ok := pwm.wantPeers[c]
	if !ok {
		peers = make(map[peer.ID]struct{}, 10)
		pwm.wantPeers[c] = peers
	}
	peers[p] = struct{}{}
	return peers
}

// Remove the peer from the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexRemove(c cid.Cid, p peer.ID) bool {
	if peers, ok := pwm.wantPeers[c]; ok {
		delete(peers, p)
		if len(peers) == 0 {
			delete(pwm.wantPeers, c)
			return true
		}
	}

	return false
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
