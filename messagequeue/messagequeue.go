package messagequeue

import (
	"context"
	"math"
	"sync"
	"time"

	debounce "github.com/bep/debounce"

	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	bswl "github.com/ipfs/go-bitswap/wantlist"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")

const (
	defaultRebroadcastInterval = 30 * time.Second
	// maxRetries is the number of times to attempt to send a message before
	// giving up
	maxRetries = 10
	// maxMessageSize is the maximum message size in bytes
	maxMessageSize = 1024 * 1024 * 2
	// sendErrorBackoff is the time to wait before retrying to connect after
	// an error when trying to send a message
	sendErrorBackoff = 100 * time.Millisecond
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = math.MaxInt32
	// sendMessageDebounce is the debounce duration when calling sendMessage()
	sendMessageDebounce = time.Millisecond
)

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error)
	Self() peer.ID
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	ctx              context.Context
	p                peer.ID
	network          MessageNetwork
	maxMessageSize   int
	sendErrorBackoff time.Duration

	signalWorkReady func()
	outgoingWork    chan struct{}
	done            chan struct{}

	// Take lock whenever any of these variables are modified
	wllock    sync.Mutex
	bcstWants *recallWantlist
	peerWants *recallWantlist
	cancels   *cid.Set
	priority  int

	// Dont touch any of these variables outside of run loop
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer
}

// recallWantlist keeps a list of current wants, and a list of all wants that
// have ever been requested
type recallWantlist struct {
	allWants *bswl.Wantlist
	current  *bswl.Wantlist
}

func newRecallWantList() *recallWantlist {
	return &recallWantlist{
		allWants: bswl.New(),
		current:  bswl.New(),
	}
}

// Add want to both the current list and the list of all wants
func (r *recallWantlist) Add(c cid.Cid, priority int, wtype pb.Message_Wantlist_WantType) {
	r.allWants.Add(c, priority, wtype)
	r.current.Add(c, priority, wtype)
}

// Remove wants from both the current list and the list of all wants
func (r *recallWantlist) Remove(c cid.Cid) {
	r.allWants.Remove(c)
	r.current.Remove(c)
}

// Remove wants by type from both the current list and the list of all wants
func (r *recallWantlist) RemoveType(c cid.Cid, wtype pb.Message_Wantlist_WantType) {
	r.allWants.RemoveType(c, wtype)
	r.current.RemoveType(c, wtype)
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork) *MessageQueue {
	return newMessageQueue(ctx, p, network, maxMessageSize, sendErrorBackoff)
}

// This constructor is used by the tests
func newMessageQueue(ctx context.Context, p peer.ID, network MessageNetwork, maxMsgSize int, sendErrorBackoff time.Duration) *MessageQueue {
	mq := &MessageQueue{
		ctx:                 ctx,
		p:                   p,
		network:             network,
		maxMessageSize:      maxMsgSize,
		bcstWants:           newRecallWantList(),
		peerWants:           newRecallWantList(),
		cancels:             cid.NewSet(),
		outgoingWork:        make(chan struct{}, 1),
		done:                make(chan struct{}),
		rebroadcastInterval: defaultRebroadcastInterval,
		sendErrorBackoff:    sendErrorBackoff,
		priority:            maxPriority,
	}

	// Apply debounce to the work ready signal (which triggers sending a message)
	debounced := debounce.New(sendMessageDebounce)
	mq.signalWorkReady = func() { debounced(mq.onWorkReady) }

	return mq
}

// Add want-haves that are part of a broadcast to all connected peers
func (mq *MessageQueue) AddBroadcastWantHaves(wantHaves []cid.Cid) {
	if len(wantHaves) == 0 {
		return
	}

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	for _, c := range wantHaves {
		mq.bcstWants.Add(c, mq.priority, pb.Message_Wantlist_Have)
		mq.priority--
	}

	// Schedule a message send
	mq.signalWorkReady()
}

// Add want-haves and want-blocks for the peer for this message queue.
func (mq *MessageQueue) AddWants(wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	if len(wantBlocks) == 0 && len(wantHaves) == 0 {
		return
	}

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	for _, c := range wantHaves {
		mq.peerWants.Add(c, mq.priority, pb.Message_Wantlist_Have)
		mq.priority--
	}
	for _, c := range wantBlocks {
		mq.peerWants.Add(c, mq.priority, pb.Message_Wantlist_Block)
		mq.priority--
	}

	// Schedule a message send
	mq.signalWorkReady()
}

// Add cancel messages for the given keys.
func (mq *MessageQueue) AddCancels(cancelKs []cid.Cid) {
	if len(cancelKs) == 0 {
		return
	}

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	for _, c := range cancelKs {
		mq.bcstWants.Remove(c)
		mq.peerWants.Remove(c)
		mq.cancels.Add(c)
	}

	// Schedule a message send
	mq.signalWorkReady()
}

// SetRebroadcastInterval sets a new interval on which to rebroadcast the full wantlist
func (mq *MessageQueue) SetRebroadcastInterval(delay time.Duration) {
	mq.rebroadcastIntervalLk.Lock()
	mq.rebroadcastInterval = delay
	if mq.rebroadcastTimer != nil {
		mq.rebroadcastTimer.Reset(delay)
	}
	mq.rebroadcastIntervalLk.Unlock()
}

// Startup starts the processing of messages and rebroadcasting.
func (mq *MessageQueue) Startup() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer = time.NewTimer(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()
	go mq.runQueue()
}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue) Shutdown() {
	close(mq.done)
}

func (mq *MessageQueue) runQueue() {
	for {
		select {
		case <-mq.rebroadcastTimer.C:
			mq.rebroadcastWantlist()
		case <-mq.outgoingWork:
			mq.sendIfReady()
		case <-mq.done:
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				_ = mq.sender.Reset()
			}
			return
		}
	}
}

// Periodically resend the list of wants to the peer
func (mq *MessageQueue) rebroadcastWantlist() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer.Reset(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()

	// If some wants were transferred from the rebroadcast list
	if mq.transferRebroadcastWants() {
		// Send them out
		mq.sendMessage()
	}
}

// Transfer wants from the rebroadcast lists into the current lists.
func (mq *MessageQueue) transferRebroadcastWants() bool {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	// Check if there are any wants to rebroadcast
	if mq.bcstWants.allWants.Len() == 0 && mq.peerWants.allWants.Len() == 0 {
		return false
	}

	// Copy all wants into current wants lists
	mq.bcstWants.current.Absorb(mq.bcstWants.allWants)
	mq.peerWants.current.Absorb(mq.peerWants.allWants)

	return true
}

func (mq *MessageQueue) onWorkReady() {
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}

func (mq *MessageQueue) sendIfReady() {
	if mq.hasPendingWants() {
		mq.sendMessage()
	}
}

func (mq *MessageQueue) sendMessage() {
	err := mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		// TODO: should we disconnect and clear the want list to avoid using up memory?
		return
	}

	// Convert want lists to a Bitswap Message
	message, onSent := mq.extractOutgoingMessage(mq.sender.SupportsHave())
	if message == nil || message.Empty() {
		return
	}

	// mq.logOutgoingMessage(message)

	// Try to send this message repeatedly
	for i := 0; i < maxRetries; i++ {
		if mq.attemptSendAndRecovery(message) {
			// We were able to send successfully.
			onSent()

			// If the message was too big and only a subset of wants could be
			// sent, schedule sending the rest of the wants in the next
			// iteration of the event loop.
			if mq.hasPendingWants() {
				mq.signalWorkReady()
			}

			return
		}
	}
}

// func (mq *MessageQueue) logOutgoingMessage(msg bsmsg.BitSwapMessage) {
// 	entries := msg.Wantlist()
// 	for _, e := range entries {
// 		if e.Cancel {
// 			if e.WantType == pb.Message_Wantlist_Have {
// 				log.Debugf("send %s->%s: cancel-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			} else {
// 				log.Debugf("send %s->%s: cancel-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			}
// 		} else {
// 			if e.WantType == pb.Message_Wantlist_Have {
// 				log.Debugf("send %s->%s: want-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			} else {
// 				log.Debugf("send %s->%s: want-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			}
// 		}
// 	}
// }

func (mq *MessageQueue) hasPendingWants() bool {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	return mq.bcstWants.current.Len() > 0 || mq.peerWants.current.Len() > 0 || mq.cancels.Len() > 0
}

func (mq *MessageQueue) extractOutgoingMessage(supportsHave bool) (bsmsg.BitSwapMessage, func()) {
	// Create a new message
	msg := bsmsg.New(false)

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	// Get broadcast and regular wantlist entries
	bcstEntries := mq.bcstWants.current.SortedEntries()
	peerEntries := mq.peerWants.current.SortedEntries()

	// Size of the message so far
	msgSize := 0

	// Add each broadcast want-have to the message
	for i := 0; i < len(bcstEntries) && msgSize < mq.maxMessageSize; i++ {
		// Broadcast wants are sent as want-have
		wantType := pb.Message_Wantlist_Have

		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// send a want-block instead
		if !supportsHave {
			wantType = pb.Message_Wantlist_Block
		}

		e := bcstEntries[i]
		msgSize += msg.AddEntry(e.Cid, e.Priority, wantType, false)
	}

	// Add each regular want-have / want-block to the message
	for i := 0; i < len(peerEntries) && msgSize < mq.maxMessageSize; i++ {
		e := peerEntries[i]
		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// don't send want-haves (only send want-blocks)
		if !supportsHave && e.WantType == pb.Message_Wantlist_Have {
			mq.peerWants.RemoveType(e.Cid, pb.Message_Wantlist_Have)
		} else {
			msgSize += msg.AddEntry(e.Cid, e.Priority, e.WantType, true)
		}
	}

	// Add each cancel to the message
	cancels := mq.cancels.Keys()
	for i := 0; i < len(cancels) && msgSize < mq.maxMessageSize; i++ {
		c := cancels[i]

		msgSize += msg.Cancel(c)

		// Clear the cancel - we make a best effort to let peers know about
		// cancels but won't save them to resend if there's a failure.
		mq.cancels.Remove(c)
	}

	// Called when the message has been successfully sent.
	// Remove the sent keys from the broadcast and regular wantlists.
	onSent := func() {
		mq.wllock.Lock()
		defer mq.wllock.Unlock()

		for _, e := range msg.Wantlist() {
			mq.bcstWants.current.Remove(e.Cid)
			mq.peerWants.current.RemoveType(e.Cid, e.WantType)
		}
	}

	return msg, onSent
}
func (mq *MessageQueue) initializeSender() error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := openSender(mq.ctx, mq.network, mq.p)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}

func (mq *MessageQueue) attemptSendAndRecovery(message bsmsg.BitSwapMessage) bool {
	err := mq.sender.SendMsg(mq.ctx, message)
	if err == nil {
		return true
	}

	log.Infof("bitswap send error: %s", err)
	_ = mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		return true
	case <-mq.ctx.Done():
		return true
	case <-time.After(mq.sendErrorBackoff):
		// wait 100ms in case disconnect notifications are still propagating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		return true
	}

	// TODO: Is this the same instance for the remote peer?
	// If its not, we should resend our entire wantlist to them
	/*
		if mq.sender.InstanceID() != mq.lastSeenInstanceID {
			wlm = mq.getFullWantlistMessage()
		}
	*/
	return false
}

func openSender(ctx context.Context, network MessageNetwork, p peer.ID) (bsnet.MessageSender, error) {
	// allow ten minutes for connections this includes looking them up in the
	// dht dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := network.ConnectTo(conctx, p)
	if err != nil {
		return nil, err
	}

	nsender, err := network.NewMessageSender(ctx, p)
	if err != nil {
		return nil, err
	}

	return nsender, nil
}
