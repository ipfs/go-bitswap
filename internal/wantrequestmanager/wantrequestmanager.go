package wantrequestmanager

import (
	"context"
	"sync"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// A message received by Bitswap
type IncomingMessage struct {
	From      peer.ID
	Blks      []blocks.Block
	Haves     []cid.Cid
	DontHaves []cid.Cid
}

// The incoming message and the set of CIDs of blocks that are wanted by a
// particular session
type messageWanted struct {
	*IncomingMessage
	Wanted *cid.Set
}

//
// The WantRequestManager keeps track of which sessions want which blocks,
// distributes incoming messages to those sessions, and writes blocks to
// the blockstore.
// When the client calls Session.WantBlocks(keys), the session creates a
// WantRequest with those keys on the WantRequestManager.
// When a message arrives Bitswap calls ReceiveMessage and the
// WantRequestManager writes the blocks to the blockstore and informs all
// Sessions that are interested in the message.
//
type WantRequestManager struct {
	bstore blockstore.Blockstore
	bpm    *bsbpm.BlockPresenceManager
	lk     sync.RWMutex
	wrs    map[cid.Cid]map[*WantRequest]struct{}
}

// New creates a new WantRequestManager
func New(bstore blockstore.Blockstore, bpm *bsbpm.BlockPresenceManager) *WantRequestManager {
	return &WantRequestManager{
		bstore: bstore,
		bpm:    bpm,
		wrs:    make(map[cid.Cid]map[*WantRequest]struct{}),
	}
}

// NewWantRequest creates a new WantRequest with the given set of wants.
func (wrm *WantRequestManager) NewWantRequest(ks []cid.Cid) (*WantRequest, error) {
	wr := newWantRequest(wrm, ks)

	wrm.lk.Lock()

	for _, c := range ks {
		// Add the WantRequest to the set of WantRequests that are interested
		// in this key
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

// getBlocks gets the blocks corresponding to the given keys from the
// blockstore
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

// ReceiveMessage sends the message to all sessions that are interested in
// it.
// Returns the set of CIDs of blocks that were wanted by a session (hadn't been
// received yet)
func (wrm *WantRequestManager) ReceiveMessage(msg *IncomingMessage) (*cid.Set, error) {
	local := msg.From == ""

	// If the message includes blocks
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
				break
			}
		}
	}
	return wanted
}

// publish the message to all WantRequests that are interested in it
// Returns the set of CIDs of blocks that were wanted by a session (hadn't been
// received yet)
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

	wanted := cid.NewSet()
	if len(interested) == 0 {
		return wanted
	}

	// Inform the BlockPresenceManager of any HAVEs / DONT_HAVEs
	wrm.bpm.ReceiveFrom(msg.From, msg.Haves, msg.DontHaves)

	// Inform interested WantRequests
	for wr := range interested {
		wrWanted := wr.receiveMessage(msg)

		// Record which WantRequests wanted the blocks in the message
		_ = wrWanted.ForEach(func(c cid.Cid) error {
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

// Keys are in the pending state until the block is received and they change to
// the received state.
// Note that the Session still needs to receive subsequent messages about wants
// that have been received, because the Session needs to discover other peers
// who had the block, so it can send requests for other wants to those peers.
type BlockState bool

const (
	BlockWanted   BlockState = true
	BlockReceived BlockState = false
)

// A WantRequest keeps track of all the wants for a Session.GetBlocks(keys)
// call.
type WantRequest struct {
	// channel of received blocks
	Out chan blocks.Block
	// reference to the WantRequestManager
	wrm *WantRequestManager
	// messages is a channel of incoming messages received by Bitswap
	messages chan *messageWanted
	// closedCh is used to signal when the WantRequest has closed, so as to
	// stop sending on the messages channel
	closedCh chan struct{}

	// Locks the variables below
	lk sync.RWMutex
	// the keys this WantRequest is interested in and their state:
	// wanted / unwanted
	ks map[cid.Cid]BlockState
	// closed indicates whether the WantRequest has been cancelled
	closed bool
}

// Creates a new WantRequest for the given keys.
func newWantRequest(wrm *WantRequestManager, ks []cid.Cid) *WantRequest {
	wr := &WantRequest{
		wrm:      wrm,
		ks:       make(map[cid.Cid]BlockState, len(ks)),
		messages: make(chan *messageWanted, len(ks)),
		Out:      make(chan blocks.Block, len(ks)),
		closedCh: make(chan struct{}),
	}

	for _, c := range ks {
		// Initialize the key state to "wanted", ie not yet received
		wr.ks[c] = BlockWanted
	}

	return wr
}

// Called when an incoming message arrives that has blocks / HAVEs / DONT_HAVEs
// for the keys in this WantRequest.
// Returns the set of CIDs of blocks that were wanted by a session (hadn't been
// received yet)
func (wr *WantRequest) receiveMessage(msg *IncomingMessage) *cid.Set {
	wr.lk.Lock()

	// If the session or want request has been cancelled, bail out
	if wr.closed {
		wr.lk.Unlock()
		return cid.NewSet()
	}

	// Filter incoming message for blocks / HAVEs / DONT_HAVEs that this
	// particular WantRequest is interested in
	blks, wanted := wr.receiveBlocks(msg.Blks)
	fmsg := &IncomingMessage{
		From:      msg.From,
		Blks:      blks,
		Haves:     wr.filterKeys(msg.Haves),
		DontHaves: wr.filterKeys(msg.DontHaves),
	}

	wr.lk.Unlock()

	// Send the message and the set of cids of wanted blocks
	select {
	case wr.messages <- &messageWanted{fmsg, wanted}:
	case <-wr.closedCh:
		return cid.NewSet()
	}

	return wanted
}

// receiveBlocks marks the corresponding wants as received, and returns
// - the list of blocks that this WantRequest was interested in
// - the set of CIDs of those blocks that the WantRequest hadn't already received
func (wr *WantRequest) receiveBlocks(blks []blocks.Block) ([]blocks.Block, *cid.Set) {
	// Filter for blocks the WantRequest is interested in
	filtered := make([]blocks.Block, 0, len(blks))

	// Work out which blocks are still pending for this WantRequest (ie this is
	// the first time the block was received)
	wanted := cid.NewSet()

	for _, b := range blks {
		c := b.Cid()
		state, ok := wr.ks[c]
		if !ok {
			continue
		}

		filtered = append(filtered, b)
		if state == BlockWanted {
			wr.ks[c] = BlockReceived
			wanted.Add(c)
		}
	}
	return filtered, wanted
}

// filterKeys returns the keys that this WantRequest is interested in
func (wr *WantRequest) filterKeys(ks []cid.Cid) []cid.Cid {
	filtered := make([]cid.Cid, 0, len(ks))
	for _, c := range ks {
		if _, ok := wr.ks[c]; ok {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// Indicates whether the WantRequest is still waiting to receive the block
// corresponding to the given key
func (wr *WantRequest) wants(c cid.Cid) bool {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	// If the session or want request has been cancelled, no keys are wanted
	if wr.closed {
		return false
	}

	state, ok := wr.ks[c]
	return ok && state == BlockWanted
}

// The set of keys that the WantRequest is interested in
func (wr *WantRequest) keys() *cid.Set {
	wr.lk.RLock()
	defer wr.lk.RUnlock()

	ks := cid.NewSet()
	for c := range wr.ks {
		ks.Add(c)
	}
	return ks
}

// When an incoming message arrives, calls receiveMessage(), and sends blocks
// on the wr.Out channel.
// When the request is cancelled, calls cancelFn with any pending wants.
func (wr *WantRequest) Run(
	sessCtx context.Context,
	ctx context.Context,
	receiveMessage func(*IncomingMessage),
	cancelFn func([]cid.Cid),
) {
	remaining := wr.keys()

	// When the function exits
	defer func() {
		// Clean up the want request
		wr.close()

		// Close the channel of outgoing blocks if it hasn't already been closed
		if remaining.Len() > 0 {
			close(wr.Out)
		}

		// Tell the session to cancel the remaining keys. Note that we want to
		// call cancelFn even if there are no remaining keys, so that the
		// session knows the WantRequest is complete.
		cancelFn(remaining.Keys())
	}()

	for {
		select {
		// Receive incoming message
		case msg := <-wr.messages:
			// Inform the session that a message has been received
			receiveMessage(msg.IncomingMessage)

			if remaining.Len() == 0 {
				break
			}

			// Send the received blocks on the channel of outgoing blocks
			for _, blk := range msg.Blks {
				// Filter for blocks that are wanted by this session
				if !msg.Wanted.Has(blk.Cid()) {
					continue
				}

				// Send block to session
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
					break
				}

				// TODO:
				// We keep listening for incoming messages even after receiving
				// all blocks, because the session wants to know about other
				// peers that have the blocks for wants from this request, so
				// it can query them for future wants.
				// If there are already several go-routines listening
				// for incoming messages for earlier requests, we can
				// probably just exit this go-routine here.
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
	close(wr.closedCh)

	wr.lk.Lock()
	wr.closed = true
	wr.lk.Unlock()

	wr.wrm.removeWantRequest(wr)
}
