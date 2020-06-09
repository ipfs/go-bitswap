package wantrequestmanager

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type IncomingMessage struct {
	From      peer.ID
	Blks      []blocks.Block
	Haves     []cid.Cid
	DontHaves []cid.Cid
}

type messageWanted struct {
	*IncomingMessage
	Wanted *cid.Set
}

//
// The WantRequestManager keeps track of WantRequests.
// When the client calls Session.WantBlocks(keys), the session creates a
// WantRequest with those keys on the WantRequestManager.
// When a message arrives Bitswap calls PublishToSessions and the
// WantRequestManager informs all Sessions that are interested in the message.
//
type WantRequestManager struct {
	bstore blockstore.Blockstore
	lk     sync.RWMutex
	wrs    map[cid.Cid]map[*WantRequest]struct{}
}

// New creates a new WantRequestManager
func New(bstore blockstore.Blockstore) *WantRequestManager {
	return &WantRequestManager{
		bstore: bstore,
		wrs:    make(map[cid.Cid]map[*WantRequest]struct{}),
	}
}

// NewWantRequest creates a new WantRequest with the given set of wants.
// cancelFn is called when the want request is cancelled with any remaining
// wants (for which blocks have not yet been received)
func (wrm *WantRequestManager) NewWantRequest(ks []cid.Cid, cancelFn func([]cid.Cid)) (*WantRequest, error) {
	wr := newWantRequest(wrm, ks, cancelFn)

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

// PublishToSessions sends the message to all sessions that are interested in
// it
func (wrm *WantRequestManager) PublishToSessions(msg *IncomingMessage) (*cid.Set, error) {
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
				continue
			}
		}
	}
	return wanted
}

// publish the message to all WantRequests that are interested in it
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

// Keys are in the wanted state until the block is received and they change to
// the unwanted state.
type ReqKeyState bool

const (
	ReqKeyWanted   ReqKeyState = true
	ReqKeyUnwanted ReqKeyState = false
)

// A WantRequest keeps track of all the wants for a Session.GetBlocks(keys)
// call.
type WantRequest struct {
	// channel of received blocks
	Out chan blocks.Block
	// reference to the WantRequestManager
	wrm *WantRequestManager
	// cancelFn is called when the WantRequest is cancelled, with the remaining
	// keys (for which blocks have not yet been received)
	cancelFn func([]cid.Cid)
	// messages is a channel of incoming messages received by Bitswap
	messages chan *messageWanted
	lk       sync.RWMutex
	// the keys this WantRequest is interested in and their state:
	// wanted / unwanted
	ks map[cid.Cid]ReqKeyState
	// closed indicates whether the WantRequest has been cancelled
	closed bool
}

// Creates a new WantRequest for the given keys.
// Calls cancelFn with keys of blocks that have not yet been received when the
// WantRequest is cancelled or the session is shutdown.
func newWantRequest(wrm *WantRequestManager, ks []cid.Cid, cancelFn func([]cid.Cid)) *WantRequest {
	wr := &WantRequest{
		wrm:      wrm,
		ks:       make(map[cid.Cid]ReqKeyState, len(ks)),
		messages: make(chan *messageWanted, len(ks)),
		cancelFn: cancelFn,
		Out:      make(chan blocks.Block, len(ks)),
	}

	for _, c := range ks {
		// Initialize the want state to "wanted"
		wr.ks[c] = ReqKeyWanted
	}

	return wr
}

// Called when an incoming message arrives that has blocks / HAVEs / DONT_HAVEs
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
	blks, wanted := wr.receiveBlocks(msg.Blks)
	fmsg := &IncomingMessage{
		From:      msg.From,
		Blks:      blks,
		Haves:     wr.filterKeys(msg.Haves),
		DontHaves: wr.filterKeys(msg.DontHaves),
	}

	wr.lk.Unlock()

	// Send the message and the set of cids of wanted blocks
	wr.messages <- &messageWanted{fmsg, wanted}

	return wanted
}

// receiveBlocks marks the corresponding wants as unwanted, and returns
// - the list of blocks that this WantRequest was interested in
// - the set of CIDs of those blocks that the WantRequest hadn't already received
func (wr *WantRequest) receiveBlocks(blks []blocks.Block) ([]blocks.Block, *cid.Set) {
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
	return ok && state == ReqKeyWanted
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

// Calls receivedMessage() with incoming messages, and sends blocks on the Out
// channel.
// When the request is cancelled, calls wr.cancelFn with any pending wants.
func (wr *WantRequest) Run(sessCtx context.Context, ctx context.Context, receiveMessage func(*IncomingMessage)) {
	remaining := wr.keys()

	// When the function exits
	defer func() {
		// Clean up the want request
		wr.close()

		if remaining.Len() == 0 {
			return
		}

		// Close the channel of outgoing blocks
		close(wr.Out)

		// Tell the session to cancel the remaining keys
		wr.cancelFn(remaining.Keys())
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
	wr.lk.Lock()
	wr.closed = true
	wr.lk.Unlock()

	wr.wrm.removeWantRequest(wr)
}
