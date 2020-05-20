package notifications

import (
	"context"
	"sync"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	peer "github.com/libp2p/go-libp2p-peer"
)

// CancelSender
type CancelSender interface {
	SendCancels(ctx context.Context, cancelKs []cid.Cid)
}

// cid => interested sessions (s.wants())
//     => cancelled

// Receive block:
// - for each session
//   - mark unwanted
//   - tell session
// - each session tells notifier to cancel
// - once all sessions are ready to cancel
//   - send cancel
//   - for each session
//     - mark cancelled

// Request cancelled (once session has removed keys):
// - remove session
// - send cancel

// Shutdown (once session shutdown)
// - remove session
// - send cancel

type IncomingMessage struct {
	From      peer.ID
	Blks      []blocks.Block
	Haves     []cid.Cid
	DontHaves []cid.Cid
}

type Notification struct {
	*IncomingMessage
	Wanted *cid.Set
}

type ReqKeyState int

const (
	ReqKeyWanted ReqKeyState = iota
	ReqKeyUnwanted
	ReqKeyCancelReady
	ReqKeyCancelled
)

type WantRequest struct {
	wrm         *WantRequestManager
	lk          sync.RWMutex
	ks          map[cid.Cid]ReqKeyState
	closed      bool
	ntfns       chan *Notification
	allReceived bool
}

type WantRequestManager struct {
	ctx              context.Context
	bstore           blockstore.Blockstore
	blockPresenceMgr *bsbpm.BlockPresenceManager
	canceller        CancelSender
	lk               sync.RWMutex
	wrs              map[cid.Cid]map[*WantRequest]struct{}
}

// New creates a new WantRequestManager
func New(ctx context.Context, bstore blockstore.Blockstore, bpm *bsbpm.BlockPresenceManager, canceller CancelSender) *WantRequestManager {
	return &WantRequestManager{
		ctx:              ctx,
		bstore:           bstore,
		blockPresenceMgr: bpm,
		canceller:        canceller,
		wrs:              make(map[cid.Cid]map[*WantRequest]struct{}),
	}
}

func (wrm *WantRequestManager) NewWantRequest(ks []cid.Cid) (*WantRequest, error) {
	wr := &WantRequest{
		wrm:   wrm,
		ks:    make(map[cid.Cid]ReqKeyState, len(ks)),
		ntfns: make(chan *Notification, len(ks)),
	}

	wrm.lk.Lock()

	for _, c := range ks {
		// Initialize the want state to "wanted"
		wr.ks[c] = ReqKeyWanted

		// Add the WantRequest to the set for this key
		wrs, ok := wrm.wrs[c]
		if !ok {
			wrs = make(map[*WantRequest]struct{})
			wrm.wrs[c] = wrs
		}
		wrs[wr] = struct{}{}
	}

	wrm.lk.Unlock()

	// Check if any of the wanted keys are already in the blockstore (unlikely
	// but possible) and if so, publish them to all WantRequests that want them
	blks, err := wr.wrm.getBlocks(ks)
	if err != nil {
		return nil, err
	}

	if len(blks) > 0 {
		wrm.publish(&IncomingMessage{Blks: blks})
	}

	return wr, nil
}

func (wrm *WantRequestManager) getBlocks(ks []cid.Cid) ([]blocks.Block, error) {
	var blks []blocks.Block
	for _, c := range ks {
		b, err := wrm.bstore.Get(c)
		if err == blockstore.ErrNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		blks = append(blks, b)
	}
	return blks, nil
}

func (wrm *WantRequestManager) OnMessageReceived(msg *IncomingMessage) (*cid.Set, error) {
	if len(msg.Blks) > 0 {
		wanted := msg.Blks
		local := msg.From == ""
		if !local {
			// If the blocks came from the network, only put blocks to the
			// blockstore if the local node actually wanted them
			wanted = wrm.wantedBlocks(msg.Blks)
		}

		if len(wanted) > 0 {
			// Put blocks to the blockstore
			err := wrm.bstore.PutMany(wanted)
			if err != nil {
				return nil, err
			}
		}
	}

	wrm.blockPresenceMgr.ReceiveFrom(msg.From, msg.Haves, msg.DontHaves)

	// Publish the message to WantRequests that are interested in the
	// blocks / HAVEs / DONT_HAVEs in the message
	return wrm.publish(msg), nil
}

// Returns blocks that are wanted by one of the sessions
func (wrm *WantRequestManager) wantedBlocks(blks []blocks.Block) []blocks.Block {
	wrm.lk.Lock()
	defer wrm.lk.Unlock()

	wanted := make([]blocks.Block, 0, len(blks))
	for _, b := range blks {
		c := b.Cid()

		// Check if the block is wanted by one of the sessions
		for wr := range wrm.wrs[c] {
			if wr.wants(c) {
				wanted = append(wanted, b)
				continue
			}
		}
	}
	return wanted
}

func (wrm *WantRequestManager) publish(msg *IncomingMessage) *cid.Set {
	wrm.lk.Lock()

	// Work out which WantRequests are interested in the message
	interested := make(map[*WantRequest]struct{}, 1) // usually only one
	for _, b := range msg.Blks {
		c := b.Cid()
		for s := range wrm.wrs[c] {
			interested[s] = struct{}{}
		}
	}
	for _, c := range msg.Haves {
		for s := range wrm.wrs[c] {
			interested[s] = struct{}{}
		}
	}
	for _, c := range msg.DontHaves {
		for s := range wrm.wrs[c] {
			interested[s] = struct{}{}
		}
	}

	wrm.lk.Unlock()

	// Inform interested WantRequests
	wanted := cid.NewSet()
	for wr := range interested {
		wrWanted := wr.receive(msg)

		// Record which WantRequests wanted the blocks in the message
		wrWanted.ForEach(func(c cid.Cid) error {
			wanted.Add(c)
			return nil
		})
	}
	return wanted
}

// Called when received blocks have been processed by the session and the
// session is ready to cancel the corresponding wants
func (wrm *WantRequestManager) onCancelReady(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}

	wrm.lk.Lock()
	defer wrm.lk.Unlock()

	wrm.cancelIfReady(ks)
}

// Called when the WantRequest has been cancelled or has completed and we want
// to cancel any blocks that have not yet been received
func (wrm *WantRequestManager) removeWantRequest(wr *WantRequest, ks []cid.Cid) {
	wrm.lk.Lock()
	defer wrm.lk.Unlock()

	wrm.cancelIfReady(ks)

	// Remove WantRequest from WantRequestManager
	for _, c := range ks {
		kwrs := wrm.wrs[c]

		delete(kwrs, wr)
		if len(kwrs) == 0 {
			delete(wrm.wrs, c)
		}
	}
}

func (wrm *WantRequestManager) cancelIfReady(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}

	// If there are no WantRequests that still want the block, the want
	// can be cancelled
	cancels := make([]cid.Cid, 0, len(ks))
	for _, c := range ks {
		if wrm.shouldSendCancel(c) {
			cancels = append(cancels, c)
			for wr := range wrm.wrs[c] {
				wr.markCancelled(c)
			}
		}
	}

	// Free up block presence tracking for keys that no session is interested
	// in anymore
	wrm.blockPresenceMgr.RemoveKeys(cancels)

	wrm.canceller.SendCancels(wrm.ctx, cancels)
}

// Check if all WantRequests no longer want the block (ie it can be cancelled)
func (wrm *WantRequestManager) shouldSendCancel(c cid.Cid) bool {
	kwrs, ok := wrm.wrs[c]
	if !ok {
		return true
	}

	readyCount := 0
	for wr := range kwrs {
		if wr.cancelReady(c) {
			readyCount++
		} else if !wr.cancelled(c) {
			// If there is a WantRequest that's not in the "ready to cancel"
			// or "already cancelled" states, don't cancel yet
			return false
		}
	}

	// All WantRequests are either ready to cancel, or have already cancelled
	// the want.
	// Only send a cancel for the want if all WantRequests haven't already
	// cancelled it.
	return readyCount > 0
}

func (wr *WantRequest) Notifications() <-chan *Notification {
	return wr.ntfns
}

// Called when a incoming message arrives that has blocks / HAVEs / DONT_HAVEs
// for the keys in this WantRequest
func (wr *WantRequest) receive(msg *IncomingMessage) *cid.Set {
	wr.lk.Lock()
	defer wr.lk.Unlock()

	wanted := cid.NewSet()
	if wr.closed {
		return wanted
	}

	// Filter incoming message for blocks / HAVEs / DONT_HAVEs that this
	// particular WantRequest is interested in
	fmsg := &IncomingMessage{
		From:      msg.From,
		Blks:      wr.filterBlocks(msg.Blks),
		Haves:     wr.filterKeys(msg.Haves),
		DontHaves: wr.filterKeys(msg.DontHaves),
	}

	// Work out which blocks are still wanted by this WantRequest (ie this is
	// the first time the block was received)
	for _, b := range fmsg.Blks {
		c := b.Cid()
		if state, ok := wr.ks[c]; ok && state == ReqKeyWanted {
			wr.ks[c] = ReqKeyUnwanted
			wanted.Add(c)
		}
	}

	// TODO: Actually we do want to keep sending notifications even after all
	// blocks are received, so that the session can hear about other peers that
	// have the blocks it's interested in

	// If all blocks have been received already, no need to send the session
	// any more notifications
	if wr.allReceived {
		return wanted
	}

	// Check if there are any blocks that the WantRequest is still waiting
	// to receive
	remaining := false
	for _, state := range wr.ks {
		if state == ReqKeyWanted {
			remaining = true
			break
		}
	}

	// Send notification with the message and the list of wanted blocks
	// (as opposed to duplicates)
	wr.ntfns <- &Notification{fmsg, wanted}

	// All blocks have been received, so close the notifications channel
	if !remaining {
		close(wr.ntfns)
		wr.allReceived = true
	}

	return wanted
}

// Called when the Session has finished processing a received block
func (wr *WantRequest) OnReceived(ks []cid.Cid) {
	wr.lk.Lock()

	if wr.closed {
		wr.lk.Unlock()
		return
	}

	for _, c := range ks {
		if state, ok := wr.ks[c]; ok && state != ReqKeyCancelled {
			wr.ks[c] = ReqKeyCancelReady
		}
	}

	wr.lk.Unlock()

	wr.wrm.onCancelReady(ks)
}

// Called when the request or session has completed
func (wr *WantRequest) OnClosing() []cid.Cid {
	wr.lk.Lock()
	defer wr.lk.Unlock()

	if wr.closed {
		return []cid.Cid{}
	}

	remaining := make([]cid.Cid, 0, len(wr.ks))
	for c, state := range wr.ks {
		if state == ReqKeyWanted {
			remaining = append(remaining, c)
			wr.ks[c] = ReqKeyUnwanted
		}
	}
	return remaining
}

func (wr *WantRequest) Close() {
	wr.lk.Lock()

	if wr.closed {
		wr.lk.Unlock()
		return
	}

	ks := make([]cid.Cid, 0, len(wr.ks))
	for c, state := range wr.ks {
		ks = append(ks, c)
		if state != ReqKeyCancelled {
			wr.ks[c] = ReqKeyCancelReady
		}
	}

	wr.closed = true

	wr.lk.Unlock()

	wr.wrm.removeWantRequest(wr, ks)
}

func (wr *WantRequest) wants(c cid.Cid) bool {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	if wr.closed {
		return false
	}

	state, ok := wr.ks[c]
	return ok && state == ReqKeyWanted
}

func (wr *WantRequest) cancelReady(c cid.Cid) bool {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	state, ok := wr.ks[c]
	return ok && state == ReqKeyCancelReady
}

func (wr *WantRequest) cancelled(c cid.Cid) bool {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	state, ok := wr.ks[c]
	return !ok || state == ReqKeyCancelled
}

func (wr *WantRequest) markCancelled(c cid.Cid) {
	wr.lk.Lock()
	defer wr.lk.Unlock()

	if _, ok := wr.ks[c]; ok {
		wr.ks[c] = ReqKeyCancelled
	}
}

func (wr *WantRequest) filterBlocks(blks []blocks.Block) []blocks.Block {
	// Note: no need to lock, this method is only called from within a lock

	filtered := make([]blocks.Block, 0, len(blks))
	for _, b := range blks {
		if _, ok := wr.ks[b.Cid()]; ok {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

func (wr *WantRequest) filterKeys(ks []cid.Cid) []cid.Cid {
	// Note: no need to lock, this method is only called from within a lock

	filtered := make([]cid.Cid, 0, len(ks))
	for _, c := range ks {
		if _, ok := wr.ks[c]; ok {
			filtered = append(filtered, c)
		}
	}
	return filtered
}
