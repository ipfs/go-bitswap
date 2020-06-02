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

type cidSessSet map[cid.Cid]map[uint64]struct{}

func newCidSessSet() cidSessSet {
	return make(cidSessSet)
}

func (css cidSessSet) has(c cid.Cid) bool {
	_, ok := css[c]
	return ok
}

func (css cidSessSet) sessionsFor(c cid.Cid) map[uint64]struct{} {
	return css[c]
}

func (css cidSessSet) keys() []cid.Cid {
	ks := make([]cid.Cid, 0, len(css))
	for c := range css {
		ks = append(ks, c)
	}
	return ks
}

func (css cidSessSet) add(c cid.Cid, sid uint64) bool {
	s, ok := css[c]
	if !ok {
		css[c] = map[uint64]struct{}{sid: struct{}{}}
		return true
	}
	s[sid] = struct{}{}
	return false
}

func (css cidSessSet) remove(c cid.Cid, sid uint64) (int, bool) {
	s, ok := css[c]
	if !ok {
		return 0, false
	}

	delete(s, sid)
	if len(s) == 0 {
		delete(css, c)
	}
	return len(s), true
}

func (css cidSessSet) len() int {
	return len(css)
}

func (css cidSessSet) sessionsCount(c cid.Cid) int {
	return len(css[c])
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

	// Keeps track of the number of active want-blocks
	wantBlockGauge Gauge
}

type peerWant struct {
	wants     map[cid.Cid]*wantInfo
	peerQueue PeerQueue
}

// New creates a new peerWantManager with a Gauge that keeps track of the
// number of active want-blocks (ie sent but no response received)
func newPeerWantManager(wantBlockGauge Gauge) *peerWantManager {
	return &peerWantManager{
		broadcastWants: newCidSessSet(),
		peerWants:      make(map[peer.ID]*peerWant),
		wantPeers:      make(map[cid.Cid]map[peer.ID]struct{}),
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
		if wi.tp == wantTypeBlock {
			// Decrement the gauge by the number of pending want-blocks to the peer
			pwm.wantBlockGauge.Dec()
		}
		// Clean up wants from the reverse index
		pwm.reverseIndexRemove(c, p)
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
		// Record that the CID was sent as a want-block
		wi, exists := pws.wants[c]
		sendWantBlock := !exists

		// If a want for this CID has already been sent
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
			// A want has not been sent for this CID, so create wantInfo
			pws.wants[c] = &wantInfo{
				tp: wantTypeBlock,
				sessions: map[uint64]struct{}{
					sid: struct{}{},
				},
			}
		}

		// Check if a want-block has already been sent to the peer
		if !sendWantBlock {
			continue
		}

		// Update the reverse index
		pwm.reverseIndexAdd(c, p)

		// Add the CID to the results
		fltWantBlks = append(fltWantBlks, c)

		// Increment the count of want-blocks
		pwm.wantBlockGauge.Inc()
	}

	// Iterate over the requested want-haves
	fltWantHvs := make([]cid.Cid, 0, len(wantHaves))
	for _, c := range wantHaves {
		// Record that the CID was sent as a want-have
		wi, exists := pws.wants[c]

		// If a want for this CID has already been sent
		if exists {
			// Add this session to the sessions who've sent the want
			wi.sessions[sid] = struct{}{}
		} else {
			// A want has not been sent for this CID, so create wantInfo
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
		pwm.reverseIndexAdd(c, p)

		// If we've already broadcasted this want, don't send another want-have.
		if pwm.broadcastWants.has(c) {
			continue
		}

		// Add the CID to the results
		fltWantHvs = append(fltWantHvs, c)
	}

	// Send the want-blocks and want-haves to the peer
	pws.peerQueue.AddWants(fltWantBlks, fltWantHvs)
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
		wantingSessCount, isBcstWant := pwm.broadcastWants.remove(c, sid)

		// If the cancel is for a broadcast want
		if isBcstWant {
			// If this was the last session that wanted the broadcast want, it's
			// ready to be removed
			bcstWants[c] = wantingSessCount == 0
		}
	}

	// Allocate a single buffer to filter cancels per peer
	cancelBuffer := make([]cid.Cid, 0, len(cancelKs))

	// Send cancels to a particular peer
	send := func(p peer.ID, pws *peerWant) {
		toCancel := cancelBuffer[:0]

		// For each cancel
		for _, c := range cancelKs {
			peerWantCancellable := false

			// Get the wantInfo for the key
			wi, isPeerWant := pws.wants[c]

			// If the key is wanted by a specific peer
			if isPeerWant {
				// If the want was from this session
				if _, ok := wi.sessions[sid]; ok {
					// Remove the session
					delete(wi.sessions, sid)

					// If this was the last session that wanted the key
					if len(wi.sessions) == 0 {
						// Send a cancel
						peerWantCancellable = true

						// If it was a want-block
						if wi.tp == wantTypeBlock {
							// Update the want gauge
							pwm.wantBlockGauge.Dec()
						}

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
