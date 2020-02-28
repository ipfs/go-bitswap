package session

import (
	"context"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const (
	// Maximum number of changes to accept before blocking
	changesBufferSize = 128
	// If the session receives this many DONT_HAVEs in a row from a peer,
	// it prunes the peer from the session
	peerDontHaveLimit = 16
)

// BlockPresence indicates whether a peer has a block.
// Note that the order is important, we decide which peer to send a want to
// based on knowing whether peer has the block. eg we're more likely to send
// a want to a peer that has the block than a peer that doesnt have the block
// so BPHave > BPDontHave
type BlockPresence int

const (
	BPDontHave BlockPresence = iota
	BPUnknown
	BPHave
)

// update encapsulates a message received by the session
type update struct {
	// Which peer sent the update
	from peer.ID
	// cids of blocks received
	ks []cid.Cid
	// HAVE message
	haves []cid.Cid
	// DONT_HAVE message
	dontHaves []cid.Cid
}

// peerAvailability indicates a peer's connection state
type peerAvailability struct {
	target    peer.ID
	available bool
}

// change can be a new peer being discovered, a new message received by the
// session, or a change in the connect status of a peer
type change struct {
	// the peer ID of a new peer
	addPeer peer.ID
	// new wants requested
	add []cid.Cid
	// new message received by session (blocks / HAVEs / DONT_HAVEs)
	update update
	// peer has connected / disconnected
	availability peerAvailability
}

type onSendFn func(to peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid)
type onPeersExhaustedFn func([]cid.Cid)

//
// sessionWantSender is responsible for sending want-have and want-block to
// peers. For each want, it sends a single optimistic want-block request to
// one peer and want-have requests to all other peers in the session.
// To choose the best peer for the optimistic want-block it maintains a list
// of how peers have responded to each want (HAVE / DONT_HAVE / Unknown) and
// consults the peer response tracker (records which peers sent us blocks).
//
type sessionWantSender struct {
	// When the context is cancelled, sessionWantSender shuts down
	ctx context.Context
	// The session ID
	sessionID uint64
	// A channel that collects incoming changes (events)
	changes chan change
	// Information about each want indexed by CID
	wants map[cid.Cid]*wantInfo
	// Keeps track of how many consecutive DONT_HAVEs a peer has sent
	peerConsecutiveDontHaves map[peer.ID]int
	// Tracks which peers we have send want-block to
	swbt *sentWantBlocksTracker
	// Maintains a list of peers and whether they are connected
	peerAvlMgr *peerAvailabilityManager
	// Tracks the number of blocks each peer sent us
	peerRspTrkr *peerResponseTracker
	// Sends wants to peers
	pm PeerManager
	// Keeps track of which peer has / doesn't have a block
	bpm *bsbpm.BlockPresenceManager
	// Called when wants are sent
	onSend onSendFn
	// Called when all peers explicitly don't have a block
	onPeersExhausted onPeersExhaustedFn
}

func newSessionWantSender(ctx context.Context, sid uint64, pm PeerManager, bpm *bsbpm.BlockPresenceManager,
	onSend onSendFn, onPeersExhausted onPeersExhaustedFn) sessionWantSender {

	spm := sessionWantSender{
		ctx:                      ctx,
		sessionID:                sid,
		changes:                  make(chan change, changesBufferSize),
		wants:                    make(map[cid.Cid]*wantInfo),
		peerConsecutiveDontHaves: make(map[peer.ID]int),
		swbt:                     newSentWantBlocksTracker(),
		peerAvlMgr:               newPeerAvailabilityManager(),
		peerRspTrkr:              newPeerResponseTracker(),

		pm:               pm,
		bpm:              bpm,
		onSend:           onSend,
		onPeersExhausted: onPeersExhausted,
	}

	return spm
}

func (spm *sessionWantSender) ID() uint64 {
	return spm.sessionID
}

// Add is called when new wants are added to the session
func (spm *sessionWantSender) Add(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}
	spm.addChange(change{add: ks})
}

// Update is called when the session receives a message with incoming blocks
// or HAVE / DONT_HAVE
func (spm *sessionWantSender) Update(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid, isNewPeer bool) {
	// fmt.Printf("Update(%s, %d, %d, %d, %t)\n", lu.P(from), len(ks), len(haves), len(dontHaves), isNewPeer)
	hasUpdate := len(ks) > 0 || len(haves) > 0 || len(dontHaves) > 0
	if !hasUpdate && !isNewPeer {
		return
	}

	ch := change{}

	if hasUpdate {
		ch.update = update{from, ks, haves, dontHaves}
	}

	// If the message came from a new peer register with the peer manager
	if isNewPeer {
		available := spm.pm.RegisterSession(from, spm)
		ch.addPeer = from
		ch.availability = peerAvailability{from, available}
	}

	spm.addChange(ch)
}

// SignalAvailability is called by the PeerManager to signal that a peer has
// connected / disconnected
func (spm *sessionWantSender) SignalAvailability(p peer.ID, isAvailable bool) {
	// fmt.Printf("SignalAvailability(%s, %t)\n", lu.P(p), isAvailable)
	availability := peerAvailability{p, isAvailable}
	spm.addChange(change{availability: availability})
}

// Run is the main loop for processing incoming changes
func (spm *sessionWantSender) Run() {
	for {
		select {
		case ch := <-spm.changes:
			spm.onChange([]change{ch})
		case <-spm.ctx.Done():
			spm.shutdown()
			return
		}
	}
}

// addChange adds a new change to the queue
func (spm *sessionWantSender) addChange(c change) {
	select {
	case spm.changes <- c:
	case <-spm.ctx.Done():
	}
}

// shutdown unregisters the session with the PeerManager
func (spm *sessionWantSender) shutdown() {
	spm.pm.UnregisterSession(spm.sessionID)
}

// collectChanges collects all the changes that have occurred since the last
// invocation of onChange
func (spm *sessionWantSender) collectChanges(changes []change) []change {
	for len(changes) < changesBufferSize {
		select {
		case next := <-spm.changes:
			changes = append(changes, next)
		default:
			return changes
		}
	}
	return changes
}

// onChange processes the next set of changes
func (spm *sessionWantSender) onChange(changes []change) {
	// Several changes may have been recorded since the last time we checked,
	// so pop all outstanding changes from the channel
	changes = spm.collectChanges(changes)

	// Apply each change
	availability := make(map[peer.ID]bool, len(changes))
	var updates []update
	for _, chng := range changes {
		// Add newly discovered peers
		if chng.addPeer != "" {
			spm.peerAvlMgr.addPeer(chng.addPeer)
		}

		// Initialize info for new wants
		for _, c := range chng.add {
			spm.trackWant(c)
		}

		// Consolidate updates and changes to availability
		if chng.update.from != "" {
			updates = append(updates, chng.update)
		}
		if chng.availability.target != "" {
			availability[chng.availability.target] = chng.availability.available
		}
	}

	// Update peer availability
	newlyAvailable, newlyUnavailable := spm.processAvailability(availability)

	// Update wants
	dontHaves := spm.processUpdates(updates)

	// Check if there are any wants for which all peers have indicated they
	// don't have the want
	spm.checkForExhaustedWants(dontHaves, newlyUnavailable)

	// If there are some connected peers, send any pending wants
	if spm.peerAvlMgr.haveAvailablePeers() {
		// fmt.Printf("sendNextWants()\n")
		spm.sendNextWants(newlyAvailable)
		// fmt.Println(spm)
	}
}

// processAvailability updates the want queue with any changes in
// peer availability
func (spm *sessionWantSender) processAvailability(availability map[peer.ID]bool) ([]peer.ID, []peer.ID) {
	var newlyAvailable []peer.ID
	var newlyUnavailable []peer.ID
	for p, isNowAvailable := range availability {
		// Make sure this is a peer that the session is actually interested in
		if wasAvailable, ok := spm.peerAvlMgr.isAvailable(p); ok {
			// If the state has changed
			if wasAvailable != isNowAvailable {
				// Update the state and record that something changed
				spm.peerAvlMgr.setPeerAvailability(p, isNowAvailable)
				// fmt.Printf("processAvailability change %s %t\n", lu.P(p), isNowAvailable)
				spm.updateWantsPeerAvailability(p, isNowAvailable)
				if isNowAvailable {
					newlyAvailable = append(newlyAvailable, p)
				} else {
					newlyUnavailable = append(newlyUnavailable, p)
				}
				// Reset the count of consecutive DONT_HAVEs received from the
				// peer
				delete(spm.peerConsecutiveDontHaves, p)
			}
		}
	}

	return newlyAvailable, newlyUnavailable
}

// isAvailable indicates whether the peer is available and whether
// it's been tracked by the Session (used by the tests)
func (spm *sessionWantSender) isAvailable(p peer.ID) (bool, bool) {
	return spm.peerAvlMgr.isAvailable(p)
}

// trackWant creates a new entry in the map of CID -> want info
func (spm *sessionWantSender) trackWant(c cid.Cid) {
	// fmt.Printf("trackWant %s\n", lu.C(c))
	if _, ok := spm.wants[c]; ok {
		return
	}

	// Create the want info
	wi := newWantInfo(spm.peerRspTrkr)
	spm.wants[c] = wi

	// For each available peer, register any information we know about
	// whether the peer has the block
	for _, p := range spm.peerAvlMgr.availablePeers() {
		spm.updateWantBlockPresence(c, p)
	}
}

// processUpdates processes incoming blocks and HAVE / DONT_HAVEs.
// It returns all DONT_HAVEs.
func (spm *sessionWantSender) processUpdates(updates []update) []cid.Cid {
	prunePeers := make(map[peer.ID]struct{})
	dontHaves := cid.NewSet()
	for _, upd := range updates {
		// TODO: If there is a timeout for the want from the peer, remove want.sentTo
		// so the want can be sent to another peer (and blacklist the peer?)
		// TODO: If a peer is no longer available, check if all providers of
		// each CID have been exhausted

		// For each DONT_HAVE
		for _, c := range upd.dontHaves {
			dontHaves.Add(c)

			// Update the block presence for the peer
			spm.updateWantBlockPresence(c, upd.from)

			// Check if the DONT_HAVE is in response to a want-block
			// (could also be in response to want-have)
			if spm.swbt.haveSentWantBlockTo(upd.from, c) {
				// If we were waiting for a response from this peer, clear
				// sentTo so that we can send the want to another peer
				if sentTo, ok := spm.getWantSentTo(c); ok && sentTo == upd.from {
					spm.setWantSentTo(c, "")
				}
			}

			// Track the number of consecutive DONT_HAVEs each peer receives
			if spm.peerConsecutiveDontHaves[upd.from] == peerDontHaveLimit {
				prunePeers[upd.from] = struct{}{}
			} else {
				spm.peerConsecutiveDontHaves[upd.from]++
			}
		}

		// For each HAVE
		for _, c := range upd.haves {
			// Update the block presence for the peer
			spm.updateWantBlockPresence(c, upd.from)
			delete(spm.peerConsecutiveDontHaves, upd.from)
		}

		// For each received block
		for _, c := range upd.ks {
			// Remove the want
			removed := spm.removeWant(c)
			if removed != nil {
				// Inform the peer tracker that this peer was the first to send
				// us the block
				spm.peerRspTrkr.receivedBlockFrom(upd.from)
			}
			delete(spm.peerConsecutiveDontHaves, upd.from)
		}
	}

	// If any peers have sent us too many consecutive DONT_HAVEs, remove them
	// from the session
	for p := range prunePeers {
		spm.SignalAvailability(p, false)
	}

	return dontHaves.Keys()
}

// checkForExhaustedWants checks if there are any wants for which all peers
// have sent a DONT_HAVE. We call these "exhausted" wants.
func (spm *sessionWantSender) checkForExhaustedWants(dontHaves []cid.Cid, newlyUnavailable []peer.ID) {
	// If there are no new DONT_HAVEs, and no peers became unavailable, then
	// we don't need to check for exhausted wants
	if len(dontHaves) == 0 && len(newlyUnavailable) == 0 {
		return
	}

	// We need to check each want for which we just received a DONT_HAVE
	wants := dontHaves

	// If a peer just became unavailable, then we need to check all wants
	// (because it may be the last peer who hadn't sent a DONT_HAVE for a CID)
	if len(newlyUnavailable) > 0 {
		// Collect all pending wants
		wants = make([]cid.Cid, len(spm.wants))
		for c := range spm.wants {
			wants = append(wants, c)
		}

		// If the last available peer in the session has become unavailable
		// then we need to broadcast all pending wants
		if len(spm.peerAvlMgr.availablePeers()) == 0 {
			spm.processExhaustedWants(wants)
			return
		}
	}

	// If all available peers for a cid sent a DONT_HAVE, signal to the session
	// that we've exhausted available peers
	if len(wants) > 0 {
		exhausted := spm.bpm.AllPeersDoNotHaveBlock(spm.peerAvlMgr.availablePeers(), wants)
		spm.processExhaustedWants(exhausted)
	}
}

// processExhaustedWants filters the list so that only those wants that haven't
// already been marked as exhausted are passed to onPeersExhausted()
func (spm *sessionWantSender) processExhaustedWants(exhausted []cid.Cid) {
	newlyExhausted := spm.newlyExhausted(exhausted)
	if len(newlyExhausted) > 0 {
		spm.onPeersExhausted(newlyExhausted)
	}
}

// convenience structs for passing around want-blocks and want-haves for a peer
type wantSets struct {
	wantBlocks *cid.Set
	wantHaves  *cid.Set
}

type allWants map[peer.ID]*wantSets

func (aw allWants) forPeer(p peer.ID) *wantSets {
	if _, ok := aw[p]; !ok {
		aw[p] = &wantSets{
			wantBlocks: cid.NewSet(),
			wantHaves:  cid.NewSet(),
		}
	}
	return aw[p]
}

// sendNextWants sends wants to peers according to the latest information
// about which peers have / dont have blocks
func (spm *sessionWantSender) sendNextWants(newlyAvailable []peer.ID) {
	toSend := make(allWants)

	for c, wi := range spm.wants {
		// Ensure we send want-haves to any newly available peers
		for _, p := range newlyAvailable {
			toSend.forPeer(p).wantHaves.Add(c)
		}

		// We already sent a want-block to a peer and haven't yet received a
		// response yet
		if wi.sentTo != "" {
			// fmt.Printf("  q - already sent want-block %s to %s\n", lu.C(c), lu.P(wi.sentTo))
			continue
		}

		// All the peers have indicated that they don't have the block
		// corresponding to this want, so we must wait to discover more peers
		if wi.bestPeer == "" {
			// TODO: work this out in real time instead of using bestP?
			// fmt.Printf("  q - no best peer for %s\n", lu.C(c))
			continue
		}

		// fmt.Printf("  q - send best: %s: %s\n", lu.C(c), lu.P(wi.bestPeer))

		// Record that we are sending a want-block for this want to the peer
		spm.setWantSentTo(c, wi.bestPeer)

		// Send a want-block to the chosen peer
		toSend.forPeer(wi.bestPeer).wantBlocks.Add(c)

		// Send a want-have to each other peer
		for _, op := range spm.peerAvlMgr.availablePeers() {
			if op != wi.bestPeer {
				toSend.forPeer(op).wantHaves.Add(c)
			}
		}
	}

	// Send any wants we've collected
	spm.sendWants(toSend)
}

// sendWants sends want-have and want-blocks to the appropriate peers
func (spm *sessionWantSender) sendWants(sends allWants) {
	// fmt.Printf(" send wants to %d peers\n", len(sends))

	// For each peer we're sending a request to
	for p, snd := range sends {
		// fmt.Printf(" send %d wants to %s\n", snd.wantBlocks.Len(), lu.P(p))

		// Piggyback some other want-haves onto the request to the peer
		for _, c := range spm.getPiggybackWantHaves(p, snd.wantBlocks) {
			snd.wantHaves.Add(c)
		}

		// Send the wants to the peer.
		// Note that the PeerManager ensures that we don't sent duplicate
		// want-haves / want-blocks to a peer, and that want-blocks take
		// precedence over want-haves.
		wblks := snd.wantBlocks.Keys()
		whaves := snd.wantHaves.Keys()
		spm.pm.SendWants(spm.ctx, p, wblks, whaves)

		// Inform the session that we've sent the wants
		spm.onSend(p, wblks, whaves)

		// Record which peers we send want-block to
		spm.swbt.addSentWantBlocksTo(p, wblks)
	}
}

// getPiggybackWantHaves gets the want-haves that should be piggybacked onto
// a request that we are making to send want-blocks to a peer
func (spm *sessionWantSender) getPiggybackWantHaves(p peer.ID, wantBlocks *cid.Set) []cid.Cid {
	var whs []cid.Cid
	for c := range spm.wants {
		// Don't send want-have if we're already sending a want-block
		// (or have previously)
		if !wantBlocks.Has(c) && !spm.swbt.haveSentWantBlockTo(p, c) {
			whs = append(whs, c)
		}
	}
	return whs
}

// newlyExhausted filters the list of keys for wants that have not already
// been marked as exhausted (all peers indicated they don't have the block)
func (spm *sessionWantSender) newlyExhausted(ks []cid.Cid) []cid.Cid {
	var res []cid.Cid
	for _, c := range ks {
		if wi, ok := spm.wants[c]; ok {
			if !wi.exhausted {
				res = append(res, c)
				wi.exhausted = true
			}
		}
	}
	return res
}

// removeWant is called when the corresponding block is received
func (spm *sessionWantSender) removeWant(c cid.Cid) *wantInfo {
	if wi, ok := spm.wants[c]; ok {
		delete(spm.wants, c)
		return wi
	}
	return nil
}

// updateWantsPeerAvailability is called when the availability changes for a
// peer. It updates all the wants accordingly.
func (spm *sessionWantSender) updateWantsPeerAvailability(p peer.ID, isNowAvailable bool) {
	for c, wi := range spm.wants {
		if isNowAvailable {
			spm.updateWantBlockPresence(c, p)
		} else {
			wi.removePeer(p)
		}
	}
}

// updateWantBlockPresence is called when a HAVE / DONT_HAVE is received for the given
// want / peer
func (spm *sessionWantSender) updateWantBlockPresence(c cid.Cid, p peer.ID) {
	wi, ok := spm.wants[c]
	if !ok {
		return
	}

	// If the peer sent us a HAVE or DONT_HAVE for the cid, adjust the
	// block presence for the peer / cid combination
	if spm.bpm.PeerHasBlock(p, c) {
		wi.setPeerBlockPresence(p, BPHave)
	} else if spm.bpm.PeerDoesNotHaveBlock(p, c) {
		wi.setPeerBlockPresence(p, BPDontHave)
	} else {
		wi.setPeerBlockPresence(p, BPUnknown)
	}
}

// Which peer was the want sent to
func (spm *sessionWantSender) getWantSentTo(c cid.Cid) (peer.ID, bool) {
	if wi, ok := spm.wants[c]; ok {
		return wi.sentTo, true
	}
	return "", false
}

// Record which peer the want was sent to
func (spm *sessionWantSender) setWantSentTo(c cid.Cid, p peer.ID) {
	if wi, ok := spm.wants[c]; ok {
		wi.sentTo = p
	}
}

// wantInfo keeps track of the information for a want
type wantInfo struct {
	// Tracks HAVE / DONT_HAVE sent to us for the want by each peer
	blockPresence map[peer.ID]BlockPresence
	// The peer that we've sent a want-block to (cleared when we get a response)
	sentTo peer.ID
	// The "best" peer to send the want to next
	bestPeer peer.ID
	// Keeps track of how many hits / misses each peer has sent us for wants
	// in the session
	peerRspTrkr *peerResponseTracker
	// true if all known peers have sent a DONT_HAVE for this want
	exhausted bool
}

// func newWantInfo(prt *peerResponseTracker, c cid.Cid, startIndex int) *wantInfo {
func newWantInfo(prt *peerResponseTracker) *wantInfo {
	return &wantInfo{
		blockPresence: make(map[peer.ID]BlockPresence),
		peerRspTrkr:   prt,
		exhausted:     false,
	}
}

// setPeerBlockPresence sets the block presence for the given peer
func (wi *wantInfo) setPeerBlockPresence(p peer.ID, bp BlockPresence) {
	wi.blockPresence[p] = bp
	wi.calculateBestPeer()

	// If a peer informed us that it has a block then make sure the want is no
	// longer flagged as exhausted (exhausted means no peers have the block)
	if bp == BPHave {
		wi.exhausted = false
	}
}

// removePeer deletes the given peer from the want info
func (wi *wantInfo) removePeer(p peer.ID) {
	// If we were waiting to hear back from the peer that is being removed,
	// clear the sentTo field so we no longer wait
	if p == wi.sentTo {
		wi.sentTo = ""
	}
	delete(wi.blockPresence, p)
	wi.calculateBestPeer()
}

// calculateBestPeer finds the best peer to send the want to next
func (wi *wantInfo) calculateBestPeer() {
	// Recalculate the best peer
	bestBP := BPDontHave
	bestPeer := peer.ID("")

	// Find the peer with the best block presence, recording how many peers
	// share the block presence
	countWithBest := 0
	for p, bp := range wi.blockPresence {
		if bp > bestBP {
			bestBP = bp
			bestPeer = p
			countWithBest = 1
		} else if bp == bestBP {
			countWithBest++
		}
	}
	wi.bestPeer = bestPeer

	// If no peer has a block presence better than DONT_HAVE, bail out
	if bestPeer == "" {
		return
	}

	// If there was only one peer with the best block presence, we're done
	if countWithBest <= 1 {
		return
	}

	// There were multiple peers with the best block presence, so choose one of
	// them to be the best
	var peersWithBest []peer.ID
	for p, bp := range wi.blockPresence {
		if bp == bestBP {
			peersWithBest = append(peersWithBest, p)
		}
	}
	wi.bestPeer = wi.peerRspTrkr.choose(peersWithBest)
}
