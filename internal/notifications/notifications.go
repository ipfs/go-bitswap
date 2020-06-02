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
	lk               sync.RWMutex
	wrs              map[cid.Cid]map[*WantRequest]struct{}
}

// New creates a new WantRequestManager
func New(ctx context.Context, bstore blockstore.Blockstore, bpm *bsbpm.BlockPresenceManager) *WantRequestManager {
	return &WantRequestManager{
		ctx:              ctx,
		bstore:           bstore,
		blockPresenceMgr: bpm,
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

func (wrm *WantRequestManager) PublishToSessions(msg *IncomingMessage) (*cid.Set, error) {
	local := msg.From == ""
	if len(msg.Blks) > 0 {
		wanted := msg.Blks
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

	if !local {
		// Inform the BlockPresenceManager of any HAVEs / DONT_HAVEs
		wrm.blockPresenceMgr.ReceiveFrom(msg.From, msg.Haves, msg.DontHaves)
	}

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

// Called when the WantRequest has been cancelled or has completed
func (wrm *WantRequestManager) removeWantRequest(wr *WantRequest) {
	wrm.lk.Lock()
	defer wrm.lk.Unlock()

	// Remove WantRequest from WantRequestManager
	for c := range wr.ks {
		kwrs := wrm.wrs[c]

		delete(kwrs, wr)
		if len(kwrs) == 0 {
			delete(wrm.wrs, c)
		}
	}
}

func (wr *WantRequest) Notifications() <-chan *Notification {
	return wr.ntfns
}

// Called when a incoming message arrives that has blocks / HAVEs / DONT_HAVEs
// for the keys in this WantRequest
func (wr *WantRequest) receive(msg *IncomingMessage) *cid.Set {
	wr.lk.Lock()

	wanted := cid.NewSet()
	if wr.closed {
		wr.lk.Unlock()
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

	wr.lk.Unlock()

	// Send notification with the message and the set of cids of wanted blocks
	wr.ntfns <- &Notification{fmsg, wanted}

	return wanted
}

func (wr *WantRequest) Close() {
	wr.lk.Lock()

	if wr.closed {
		wr.lk.Unlock()
		return
	}

	wr.closed = true

	wr.lk.Unlock()

	wr.wrm.removeWantRequest(wr)
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
