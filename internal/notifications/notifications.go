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

type notification struct {
	*IncomingMessage
	Wanted *cid.Set
}

type ReqKeyState bool

const (
	ReqKeyWanted   ReqKeyState = true
	ReqKeyUnwanted ReqKeyState = false
)

type WantRequest struct {
	Out      chan blocks.Block
	wrm      *WantRequestManager
	cancelFn func([]cid.Cid)
	ntfns    chan *notification
	lk       sync.RWMutex
	ks       map[cid.Cid]ReqKeyState
	closed   bool
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

func (wrm *WantRequestManager) NewWantRequest(ks []cid.Cid, cancelFn func([]cid.Cid)) (*WantRequest, error) {
	wr := newWantRequest(wrm, ks, cancelFn)

	wrm.lk.Lock()

	for _, c := range ks {
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
	blks, err := wrm.getBlocks(ks)
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
		// TODO: do this in parallel?
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
	wrm.lk.RLock()

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

	wrm.lk.RUnlock()

	// Inform interested WantRequests
	wanted := cid.NewSet()
	for wr := range interested {
		wrWanted := wr.receiveMessage(msg)

		// Record which WantRequests wanted the blocks in the message
		wrWanted.ForEach(func(c cid.Cid) error {
			wanted.Add(c)
			return nil
		})
	}
	return wanted
}

// Called when the WantRequest has been closed
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

// Creates a new WantRequest for the given keys.
// Calls cancelFn with keys of blocks that have not yet been received when the
// WantRequest is cancelled or the session is shutdown.
func newWantRequest(wrm *WantRequestManager, ks []cid.Cid, cancelFn func([]cid.Cid)) *WantRequest {
	wr := &WantRequest{
		wrm:      wrm,
		ks:       make(map[cid.Cid]ReqKeyState, len(ks)),
		ntfns:    make(chan *notification, len(ks)),
		cancelFn: cancelFn,
		Out:      make(chan blocks.Block),
	}

	for _, c := range ks {
		// Initialize the want state to "wanted"
		wr.ks[c] = ReqKeyWanted
	}

	return wr
}

// Called when a incoming message arrives that has blocks / HAVEs / DONT_HAVEs
// for the keys in this WantRequest
func (wr *WantRequest) receiveMessage(msg *IncomingMessage) *cid.Set {
	wr.lk.Lock()

	// If the session or want request has been cancelled, bail out
	if wr.closed {
		wr.lk.Unlock()
		return cid.NewSet()
	}

	// Filter incoming message for blocks / HAVEs / DONT_HAVEs that this
	// particular WantRequest is interested in
	blks, wanted := wr.filterBlocks(msg.Blks)
	fmsg := &IncomingMessage{
		From:      msg.From,
		Blks:      blks,
		Haves:     wr.filterKeys(msg.Haves),
		DontHaves: wr.filterKeys(msg.DontHaves),
	}

	wr.lk.Unlock()

	// Send notification with the message and the set of cids of wanted blocks
	wr.ntfns <- &notification{fmsg, wanted}

	return wanted
}

func (wr *WantRequest) filterBlocks(blks []blocks.Block) ([]blocks.Block, *cid.Set) {
	// Filter for blocks the WantRequest is interested in
	filtered := make([]blocks.Block, 0, len(blks))

	// Work out which blocks are still wanted by this WantRequest (ie this is
	// the first time the block was received)
	wanted := cid.NewSet()

	for _, b := range blks {
		c := b.Cid()
		state, ok := wr.ks[c]
		if !ok {
			continue
		}

		filtered = append(filtered, b)
		if state == ReqKeyWanted {
			wr.ks[c] = ReqKeyUnwanted
			wanted.Add(c)
		}
	}
	return filtered, wanted
}

func (wr *WantRequest) filterKeys(ks []cid.Cid) []cid.Cid {
	filtered := make([]cid.Cid, 0, len(ks))
	for _, c := range ks {
		if _, ok := wr.ks[c]; ok {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

func (wr *WantRequest) wants(c cid.Cid) bool {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	// If the session or want request has been cancelled, no keys are wanted
	if wr.closed {
		return false
	}

	state, ok := wr.ks[c]
	return ok && state == ReqKeyWanted
}

func (wr *WantRequest) keys() *cid.Set {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	ks := cid.NewSet()
	for c := range wr.ks {
		ks.Add(c)
	}
	return ks
}

func (wr *WantRequest) Run(sessCtx context.Context, ctx context.Context, receiveMessage func(*IncomingMessage)) {
	remaining := wr.keys()

	chClosed := false

	// When the function exits
	defer func() {
		// Clean up the want request
		wr.close()

		// Close the channel of outgoing blocks
		if !chClosed {
			close(wr.Out)
		}

		// Tell the session to cancel the remaining keys
		wr.cancelFn(remaining.Keys())
	}()

	for {
		select {
		// Receive incoming message
		case msg := <-wr.ntfns:
			// Inform the session that a message has been received
			receiveMessage(msg.IncomingMessage)

			if chClosed {
				break
			}

			// Send the received blocks on the channel of outgoing blocks
			for _, blk := range msg.Blks {
				// Filter for blocks that are wanted by this session
				if !msg.Wanted.Has(blk.Cid()) {
					continue
				}

				select {
				case wr.Out <- blk:
				case <-ctx.Done():
					return
				case <-sessCtx.Done():
					return
				}

				// Received all requested blocks so close the outgoing channel
				remaining.Remove(blk.Cid())
				if remaining.Len() == 0 {
					close(wr.Out)
					chClosed = true
					break

					// TODO:
					// We keep listening for incoming messages as the
					// session wants to know about other peers that have
					// the blocks for wants from this request, so it can
					// query them for wants requested in future.
					// If there are already several go-routines listening
					// for incoming messages for earlier requests, we can
					// exit this go-routine now.
				}
			}

		// If the want request is cancelled, bail out
		case <-ctx.Done():
			return

		// If the session is shut down, bail out
		case <-sessCtx.Done():
			return
		}
	}
}

// Close the WantRequest, and remove it from the WantRequestManager
func (wr *WantRequest) close() {
	wr.lk.Lock()
	wr.closed = true
	wr.lk.Unlock()

	wr.wrm.removeWantRequest(wr)
}
