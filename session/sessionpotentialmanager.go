package session

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	lu "github.com/ipfs/go-bitswap/logutil"

	cid "github.com/ipfs/go-cid"
	pq "github.com/ipfs/go-ipfs-pq"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Amount to increase potential by if we received a HAVE message
const rcvdHavePotentialGain = 0.8

type update struct {
	from      peer.ID
	ks        []cid.Cid
	haves     []cid.Cid
	dontHaves []cid.Cid
}

type change struct {
	addPeer      peer.ID
	add          []cid.Cid
	update       *update
	availability map[peer.ID]bool
}

type onSendFn func(peer.ID, []cid.Cid, []cid.Cid)

type sessionPotentialManager struct {
	ctx            context.Context
	sessionID      uint64
	changes        chan change
	sendTrigger    chan struct{}
	sentPotential  map[cid.Cid]float64
	wants          map[cid.Cid]*wantPotential
	pastWants      *cid.Set
	queue          pq.PQ
	availablePeers map[peer.ID]bool

	pm  PeerManager
	bpm *bsbpm.BlockPresenceManager
	// bsm                   BlockSentManager
	potentialThresholdMgr *potentialThresholdManager

	onSend onSendFn
	// maxPeerHaves int
}

func newSessionPotentialManager(sid uint64, pm PeerManager, bpm *bsbpm.BlockPresenceManager, onSend onSendFn) sessionPotentialManager {
	spm := sessionPotentialManager{
		sessionID:      sid,
		changes:        make(chan change, 128),
		sendTrigger:    make(chan struct{}, 1),
		sentPotential:  make(map[cid.Cid]float64),
		wants:          make(map[cid.Cid]*wantPotential),
		pastWants:      cid.NewSet(),
		availablePeers: make(map[peer.ID]bool),

		pm:                    pm,
		bpm:                   bpm,
		potentialThresholdMgr: newPotentialThresholdManager(),
		onSend:                onSend,
		// maxPeerHaves:          16,
	}
	spm.queue = pq.New(WrapCompare(spm.WantCompare))
	return spm
}

func (spm *sessionPotentialManager) ID() uint64 {
	return spm.sessionID
}

func (spm *sessionPotentialManager) Add(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}
	spm.changes <- change{add: ks}
}

func (spm *sessionPotentialManager) Update(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid, isNewPeer bool) {
	// fmt.Printf("Update(%s, %d, %d, %d, %t)", lu.P(from), len(ks), len(haves), len(dontHaves), isNewPeer)
	hasUpdate := len(ks) > 0 || len(haves) > 0 || len(dontHaves) > 0
	if !hasUpdate && !isNewPeer {
		return
	}

	ch := change{}

	if hasUpdate {
		ch.update = &update{from, ks, haves, dontHaves}
	}

	if isNewPeer {
		availability := make(map[peer.ID]bool)
		availability[from] = spm.pm.RegisterSession(from, spm)
		ch.addPeer = from
		ch.availability = availability
	}

	spm.changes <- ch
}

func (spm *sessionPotentialManager) SignalAvailability(p peer.ID, isAvailable bool) {
	// fmt.Printf("SignalAvailability(%s, %t)", lu.P(p), isAvailable)

	availability := make(map[peer.ID]bool)
	availability[p] = isAvailable
	spm.changes <- change{availability: availability}
}

func (spm *sessionPotentialManager) triggerSend() {
	select {
	case spm.sendTrigger <- struct{}{}:
	default:
	}
}

func (spm *sessionPotentialManager) Run(ctx context.Context) {
	spm.ctx = ctx

	// TODO: use a second thread to consolidate changes then pass them to the main thread?
	// (this would prevent spm.changes from ever filling up, and blocking the caller)
	for {
		select {
		case ch := <-spm.changes:
			spm.onChange([]change{ch})
		case <-spm.sendTrigger:
			spm.onChange(nil)
		case <-ctx.Done():
			spm.shutdown()
			return
		}
	}
}

func (spm *sessionPotentialManager) shutdown() {
	spm.pm.UnregisterSession(spm.sessionID)
}

func (spm *sessionPotentialManager) collectChanges(changes []change) []change {
	for {
		select {
		case next := <-spm.changes:
			changes = append(changes, next)
		default:
			return changes
		}
	}
}

func (spm *sessionPotentialManager) onChange(changes []change) {
	// Several changes may have been recorded since the last time we checked,
	// so pop all outstanding changes from the channel
	changes = spm.collectChanges(changes)

	// Apply each change
	availability := make(map[peer.ID]bool)
	var updates []update
	for _, chng := range changes {
		// Add newly discovered peers
		if chng.addPeer != "" {
			spm.addPeer(chng.addPeer)
		}

		// Initialize potential for new wants
		for _, c := range chng.add {
			spm.trackPotential(c)
		}

		// Consolidate updates and changes to availability
		if chng.update != nil {
			updates = append(updates, *chng.update)
		}
		if chng.availability != nil {
			for p, isAvailable := range chng.availability {
				availability[p] = isAvailable
			}
		}
	}

	// Update peer availability
	spm.processAvailability(availability)

	// Update wants
	spm.processUpdates(updates)

	// If there are some connected peers, send any pending wants
	if spm.haveAvailablePeers() {
		// fmt.Printf("sendNextWants()\n")
		spm.sendNextWants()
		// fmt.Println(spm)
	}
}

func (spm *sessionPotentialManager) processAvailability(availability map[peer.ID]bool) {
	changed := false
	for p, isNowAvailable := range availability {
		// Make sure this is a peer that the session is actually interested in
		if wasAvailable, ok := spm.availablePeers[p]; ok {
			// If the state has changed
			if wasAvailable != isNowAvailable {
				// Update the state and record that something changed
				spm.availablePeers[p] = isNowAvailable
				changed = true
				// fmt.Printf("processAvailability change %s %t\n", lu.P(p), isNowAvailable)
				spm.updatePotentialAvailability(p, isNowAvailable)
			}
		}
	}

	// If availability has changed, update the queue
	if changed {
		spm.requeueAll()
	}
}

func (spm *sessionPotentialManager) trackPotential(c cid.Cid) {
	if _, ok := spm.wants[c]; ok {
		return
	}

	wp := newWantPotential(c, len(spm.wants))
	spm.wants[c] = wp

	for p, isAvailable := range spm.availablePeers {
		if isAvailable {
			spm.updatePotential(c, p)
		}
	}

	spm.queue.Push(wp)
}

func (spm *sessionPotentialManager) processUpdates(updates []update) {
	changed := cid.NewSet()
	uniqs := cid.NewSet()
	dups := cid.NewSet()
	for _, upd := range updates {
		// TODO: If there is a timeout for the want from the peer, remove the want
		// potential so it can be sent again (and blacklist the peer?)

		// For each DONT_HAVE
		for _, c := range upd.dontHaves {
			// Remove the sent potential for the peer / want
			spm.removeSentPotential(c, upd.from)
			// Update the want potential for the peer
			spm.updatePotential(c, upd.from)
			changed.Add(c)
		}

		// For each HAVE
		for _, c := range upd.haves {
			// Update the want potential for the peer
			spm.updatePotential(c, upd.from)
			changed.Add(c)
		}

		// For each received block
		for _, c := range upd.ks {
			// Move the want to the set of past wants
			removed := spm.removeWant(c)
			if removed != nil {
				spm.pastWants.Add(c)

				// We're just going to remove this want the next time it's
				// popped from the queue, but we want to update its position
				// so as to move it to the front of the queue (so it gets
				// removed sooner, freeing up memory)
				spm.queue.Update(removed.Index())
			}

			// Only record uniqs / dups if the block came from the network
			// (as opposed to coming from the local node)
			if upd.from != "" {
				if removed != nil {
					uniqs.Add(c)
				} else if spm.pastWants.Has(c) {
					dups.Add(c)
				}
			}
		}
	}

	// Update the potential threshold
	spm.potentialThresholdMgr.ReceivedBlocks(uniqs.Keys(), dups.Keys())

	// For each want that changed
	for _, c := range changed.Keys() {
		// Update the want's position in the queue
		if wp, ok := spm.wants[c]; ok {
			spm.queue.Update(wp.Index())
		}
	}
}

func (spm *sessionPotentialManager) IdleTimeout() {
	spm.potentialThresholdMgr.IdleTimeout()
}

type allWants struct {
	wantBlocks *cid.Set
	wantHaves  *cid.Set
}

func (spm *sessionPotentialManager) sendNextWants() {
	toSend := make(map[peer.ID]*allWants)
	getSendBuffer := func(p peer.ID) *allWants {
		if _, ok := toSend[p]; !ok {
			toSend[p] = &allWants{
				wantBlocks: cid.NewSet(),
				wantHaves:  cid.NewSet(),
			}
		}
		return toSend[p]
	}

	var skipped []*wantPotential

	// At the end
	onComplete := func() {
		// Send any wants we've collected so far
		spm.sendWants(toSend)

		// Put skipped wants back into the queue
		for _, wp := range skipped {
			spm.queue.Push(wp)
		}
	}

	// Go through the queue of wants
	potentialThreshold := spm.potentialThresholdMgr.PotentialThreshold()
	// fmt.Printf("  q(%d)\n", spm.queue.Len())
	for spm.queue.Len() > 0 {
		// Pop the next item off the queue
		wp := spm.queue.Pop().(*wantPotential)

		// If the want has been removed (because we've already received the
		// block) just ignore the want
		if _, ok := spm.wants[wp.want]; !ok {
			// fmt.Printf("want %s deleted\n", lu.C(wp.want))
			continue
		}

		// We've processed all of the wants with some potential, so we can
		// stop here
		if wp.bestPotential <= 0 {
			// fmt.Printf("    q - best: %s: %s/%.2f has no best potential, stopping\n",
			// 	lu.C(wp.want), lu.P(wp.bestPeer), wp.bestPotential)

			// Put the item back in the queue
			spm.queue.Push(wp)

			// Send any wants we've collected
			onComplete()

			return
		}

		// If the want's sent potential is already above the threshold
		if wp.sentPotential > potentialThreshold {
			// fmt.Printf("    q - best: %s: %s/%.2f has sent potential %.2f > threshold %.2f skipping\n",
			// 	lu.C(wp.want), lu.P(wp.bestPeer), wp.bestPotential, wp.sentPotential, potentialThreshold)
			// Skip the want, we'll put it back in the queue later
			skipped = append(skipped, wp)
			continue
		}

		// If the peer is not available, the state has changed so we need to
		// start again
		isAvailable, pok := spm.availablePeers[wp.bestPeer]
		if !pok || !isAvailable || !spm.pm.RequestToken(wp.bestPeer) {
			// Mark the peer as unavailable
			spm.setPeerAvailability(wp.bestPeer, false)

			// Put the item back in the queue
			spm.queue.Push(wp)

			// Send any wants we've collected
			onComplete()

			// Restart the process
			spm.triggerSend()

			return
		}

		// fmt.Printf("  q - send best: %s: %s/%.2f\n", lu.C(wp.want), lu.P(wp.bestPeer), wp.bestPotential)

		// Add the sent potential and clear the want potential for the want
		sentPeer := wp.bestPeer
		potential := spm.clearPotential(wp.want, sentPeer)
		spm.addSentPotential(wp.want, sentPeer, potential)

		// If the want still has some potential and its sent potential is still
		// below the threshold
		if wp.bestPotential > 0 && wp.sentPotential <= potentialThreshold {
			// fmt.Printf("    q - best: %s: %s/%.2f has sent potential %.2f <= threshold %.2f putting back in q\n",
			// 	lu.C(wp.want), lu.P(wp.bestPeer), wp.bestPotential, wp.sentPotential, potentialThreshold)
			// Put it back in the queue
			spm.queue.Push(wp)
		} else {
			// fmt.Printf("    q - best: %s: %s/%.2f (sent potential %.2f vs threshold %.2f). Skipping\n",
			// 	lu.C(wp.want), lu.P(wp.bestPeer), wp.bestPotential, wp.sentPotential, potentialThreshold)
			// Skip the want, we'll put it back in the queue later
			skipped = append(skipped, wp)
		}

		// Send a want-block to the chosen peer
		getSendBuffer(sentPeer).wantBlocks.Add(wp.want)

		// Send a want-have with this CID to each other peer
		// TODO: limit the number of other peers we send want-have to?
		for op := range spm.availablePeers {
			if op != sentPeer {
				getSendBuffer(op).wantHaves.Add(wp.want)
			}
		}
	}

	// Send any wants we've collected
	onComplete()
}

func (spm *sessionPotentialManager) sendWants(sends map[peer.ID]*allWants) {
	// fmt.Printf(" send wants to %d peers\n", len(sends))
	sentWantBlocks := cid.NewSet()

	// For each peer we're sending a request to
	for p, snd := range sends {
		// fmt.Printf(" send %d wants to %s\n", snd.wantBlocks.Len(), lu.P(p))
		// Add the sent potential and clear the want potential for the want
		for _, c := range snd.wantBlocks.Keys() {
			// potential := spm.clearPotential(c, p)
			// spm.addSentPotential(c, p, potential)
			sentWantBlocks.Add(c)
		}

		// Piggyback some other want-haves onto the request
		for _, c := range spm.getPiggybackWantHaves(p) {
			snd.wantHaves.Add(c)
		}

		// Send the wants to the peer
		wblks := snd.wantBlocks.Keys()
		whaves := snd.wantHaves.Keys()
		spm.pm.SendWants(spm.ctx, p, wblks, whaves)
		spm.onSend(p, wblks, whaves)
	}

	// For each changed want
	ks := sentWantBlocks.Keys()
	for _, c := range ks {
		// Update the want's position in the queue
		if wp, ok := spm.wants[c]; ok {
			spm.queue.Update(wp.Index())
		}
	}
}

func (spm *sessionPotentialManager) getPiggybackWantHaves(p peer.ID) []cid.Cid {
	// TODO: Should do this in a smarter way (eg choose the most recent that haven't been sent to this peer)
	var whs []cid.Cid
	for c := range spm.wants {
		whs = append(whs, c)
	}
	return whs
}

func (spm *sessionPotentialManager) addPeer(p peer.ID) {
	spm.availablePeers[p] = false
}

func (spm *sessionPotentialManager) setPeerAvailability(p peer.ID, isAvailable bool) {
	spm.availablePeers[p] = isAvailable
	spm.updatePotentialAvailability(p, isAvailable)
	spm.requeueAll()
}

func (spm *sessionPotentialManager) haveAvailablePeers() bool {
	for _, isAvailable := range spm.availablePeers {
		if isAvailable {
			return true
		}
	}
	return false
}

func (spm *sessionPotentialManager) requeueAll() {
	// TODO: if this performs poorly, may need to only update rows where
	// best potential has changed
	for _, wp := range spm.wants {
		spm.queue.Update(wp.Index())
	}
}

func (spm *sessionPotentialManager) removeWant(c cid.Cid) *wantPotential {
	if wp, ok := spm.wants[c]; ok {
		delete(spm.wants, c)
		return wp
	}
	return nil
}

func (spm *sessionPotentialManager) clearPotential(c cid.Cid, p peer.ID) float64 {
	if wp, ok := spm.wants[c]; ok {
		return wp.clearPeerPotential(p)
	}
	return 0.0
}

func (spm *sessionPotentialManager) updatePotential(c cid.Cid, p peer.ID) {
	wp, ok := spm.wants[c]
	if !ok {
		return
	}

	potential := 0.0
	// If the peer sent us a HAVE or HAVE_NOT for the cid, adjust the
	// potential for the peer / cid combination
	if spm.bpm.PeerHasBlock(p, c) {
		potential += rcvdHavePotentialGain
	} else if spm.bpm.PeerDoesNotHaveBlock(p, c) {
		potential -= rcvdHavePotentialGain
	} else {
		// TODO: Take into account peer's uniq / dup ratio?
		potential += 0.5
	}

	wp.setPeerPotential(p, potential)
}

func (spm *sessionPotentialManager) updatePotentialAvailability(p peer.ID, isNowAvailable bool) {
	for c, wp := range spm.wants {
		if isNowAvailable {
			spm.updatePotential(c, p)
		} else {
			wp.removePeerPotential(p)
			spm.removeSentPotential(c, p)
		}
	}
}

func (spm *sessionPotentialManager) addSentPotential(c cid.Cid, p peer.ID, gain float64) {
	if wp, ok := spm.wants[c]; ok {
		if existing, sok := wp.sent[p]; sok {
			wp.sentPotential -= existing
		}
		wp.sent[p] = gain
		wp.sentPotential += gain
	}
}

func (spm *sessionPotentialManager) removeSentPotential(c cid.Cid, p peer.ID) {
	if wp, ok := spm.wants[c]; ok {
		if existing, sok := wp.sent[p]; sok {
			delete(wp.sent, p)
			wp.sentPotential -= existing
		}
	}
}

func (spm *sessionPotentialManager) String() string {
	var b bytes.Buffer
	var availablePeers []peer.ID
	for p := range spm.availablePeers {
		availablePeers = append(availablePeers, p)
	}

	b.WriteString("         best |")
	for _, p := range availablePeers {
		b.WriteString(" ")
		b.WriteString(lu.P(p))
	}
	b.WriteString(" | sent:")
	for _, p := range availablePeers {
		b.WriteString(" ")
		b.WriteString(lu.P(p))
	}
	b.WriteString("\n")

	for c, wp := range spm.wants {
		b.WriteString(fmt.Sprintf("%s:  %4.2f |", lu.C(c), wp.bestPotential))
		for _, p := range availablePeers {
			b.WriteString(fmt.Sprintf("   %4.2f", wp.byPeer[p]))
		}

		b.WriteString(fmt.Sprintf(" | %4.2f ", wp.sentPotential))
		for _, p := range availablePeers {
			if potential, ok := wp.sent[p]; ok {
				b.WriteString(fmt.Sprintf("  %4.2f", potential))
			} else {
				b.WriteString("   --  ")
			}
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
	return b.String()
}

type wantPotential struct {
	startIndex    int
	index         int
	want          cid.Cid
	byPeer        map[peer.ID]float64
	sent          map[peer.ID]float64
	sentPotential float64
	bestPotential float64
	bestPeer      peer.ID
}

func newWantPotential(c cid.Cid, startIndex int) *wantPotential {
	return &wantPotential{
		startIndex: startIndex,
		want:       c,
		byPeer:     make(map[peer.ID]float64),
		sent:       make(map[peer.ID]float64),
	}
}

// Required to implement the Elem interface for pq
func (wp *wantPotential) SetIndex(i int) {
	wp.index = i
}

// Required to implement the Elem interface for pq
func (wp *wantPotential) Index() int {
	return wp.index
}

func (wp *wantPotential) setPeerPotential(p peer.ID, potential float64) {
	wp.byPeer[p] = potential
	wp.calculateBestPeer()
}

func (wp *wantPotential) clearPeerPotential(p peer.ID) float64 {
	if potential, ok := wp.byPeer[p]; ok {
		wp.byPeer[p] = 0
		wp.calculateBestPeer()
		return potential
	}
	return 0
}

func (wp *wantPotential) removePeerPotential(p peer.ID) {
	delete(wp.byPeer, p)
	wp.calculateBestPeer()
}

func (wp *wantPotential) calculateBestPeer() {
	// Recalculate the best peer
	wp.bestPeer = ""
	wp.bestPotential = 0
	for p, potential := range wp.byPeer {
		isBest := potential > wp.bestPotential || (potential == wp.bestPotential && rand.Intn(2) == 0)
		if wp.bestPeer == "" || isBest {
			wp.bestPotential = potential
			wp.bestPeer = p
		}
	}
	if wp.bestPotential <= 0 {
		wp.bestPeer = ""
	}
}

// Order by
// 1. best potential desc
// 2. sent potential asc
// 3. FIFO
func (spm *sessionPotentialManager) WantCompare(a, b *wantPotential) bool {
	// Move deleted wants to the front of the queue so they are popped off
	// first, freeing up memory
	if _, aok := spm.wants[a.want]; !aok {
		return true
	}
	if _, bok := spm.wants[b.want]; !bok {
		return false
	}

	if a.bestPotential == b.bestPotential {
		if a.sentPotential == b.sentPotential {
			return a.startIndex < b.startIndex
		}
		return a.sentPotential < b.sentPotential
	}
	return a.bestPotential > b.bestPotential
}

// WrapCompare wraps a wantPotential comparison function so it can be used as
// comparison for a priority queue
func WrapCompare(f func(a, b *wantPotential) bool) func(a, b pq.Elem) bool {
	return func(a, b pq.Elem) bool {
		return f(a.(*wantPotential), b.(*wantPotential))
	}
}
