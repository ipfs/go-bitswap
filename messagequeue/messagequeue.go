package messagequeue

import (
	"context"
	"math"
	"sync"
	"time"

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
	maxRetries                 = 10
	// Maximum message size in bytes
	maxMessageSize = 1024 * 1024 * 2
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = math.MaxInt32
	// sendMessageDebounce is the debounce duration when calling sendMessage():
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
	peerSupportsHave bool

	outgoingWork chan struct{}
	done         chan struct{}

	// do not touch out of run loop
	wlbcst                *bswl.Wantlist
	wlpeer                *bswl.Wantlist
	nextMessage           bsmsg.BitSwapMessage
	nextMessageLk         sync.RWMutex
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer
	priority              int
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, supportsHave bool, network MessageNetwork) *MessageQueue {
	return newMessageQueue(ctx, p, supportsHave, network, maxMessageSize)
}

// This constructor is used by the tests
func newMessageQueue(ctx context.Context, p peer.ID, supportsHave bool, network MessageNetwork, maxMsgSize int) *MessageQueue {
	return &MessageQueue{
		ctx:                 ctx,
		p:                   p,
		network:             network,
		maxMessageSize:      maxMsgSize,
		peerSupportsHave:    supportsHave,
		wlbcst:              bswl.New(),
		wlpeer:              bswl.New(),
		outgoingWork:        make(chan struct{}, 1),
		done:                make(chan struct{}),
		rebroadcastInterval: defaultRebroadcastInterval,
		priority:            maxPriority,
	}
}

func (mq *MessageQueue) AddBroadcastWantHaves(wantHaves []cid.Cid) {
	mq.addMessageEntries(func() {
		sendDontHave := false
		for _, c := range wantHaves {
			wantType := pb.Message_Wantlist_Have
			// If the remote peer doesn't support HAVE / DONT_HAVE messages,
			// send a block instead
			if !mq.peerSupportsHave {
				wantType = pb.Message_Wantlist_Block
			}
			mq.nextMessage.AddEntry(c, mq.priority, wantType, sendDontHave)
			mq.wlbcst.Add(c, mq.priority, wantType)
			mq.priority--
		}
	})
}

func (mq *MessageQueue) AddWants(wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	mq.addMessageEntries(func() {
		sendDontHave := true
		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// don't send want-haves (only send want-blocks)
		if mq.peerSupportsHave {
			for _, c := range wantHaves {
				mq.nextMessage.AddEntry(c, mq.priority, pb.Message_Wantlist_Have, sendDontHave)
				mq.wlpeer.Add(c, mq.priority, pb.Message_Wantlist_Have)
				mq.priority--
			}
		}
		for _, c := range wantBlocks {
			mq.nextMessage.AddEntry(c, mq.priority, pb.Message_Wantlist_Block, sendDontHave)
			mq.wlpeer.Add(c, mq.priority, pb.Message_Wantlist_Block)
			mq.priority--
		}
	})
}

func (mq *MessageQueue) AddCancels(cancelKs []cid.Cid) {
	mq.addMessageEntries(func() {
		for _, c := range cancelKs {
			mq.nextMessage.Cancel(c)

			mq.wlbcst.Remove(c, pb.Message_Wantlist_Block)
			mq.wlpeer.Remove(c, pb.Message_Wantlist_Block)
		}
	})
}

func (mq *MessageQueue) addMessageEntries(addEntriesFn func()) {
	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()

	// if we have no message held allocate a new one
	if mq.nextMessage == nil {
		mq.nextMessage = bsmsg.New(false)
	}

	addEntriesFn()

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

// Startup starts the processing of messages, and creates an initial message
// based on the given initial wantlist.
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
	debounceSendMessage := Debounce(sendMessageDebounce, mq.sendMessage)

	for {
		select {
		case <-mq.rebroadcastTimer.C:
			mq.rebroadcastWantlist()
		case <-mq.outgoingWork:
			// mq.sendMessage()
			debounceSendMessage()
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

	// Check if there are any wants to rebroadcast
	if mq.wlbcst.Len() == 0 && mq.wlpeer.Len() == 0 {
		return
	}

	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()

	// Create a new message, if necessary
	if mq.nextMessage == nil {
		mq.nextMessage = bsmsg.New(false)
	}

	// Add each broadcast want-have to the message
	for _, e := range mq.wlbcst.Entries() {
		mq.nextMessage.AddEntry(e.Cid, e.Priority, e.WantType, false)
	}
	// Add each regular want-have / want-block to the message
	for _, e := range mq.wlpeer.Entries() {
		mq.nextMessage.AddEntry(e.Cid, e.Priority, e.WantType, true)
	}

	// Schedule a message send
	mq.signalWorkReady()
}

func (mq *MessageQueue) extractOutgoingMessage() (bsmsg.BitSwapMessage, bool) {
	// grab outgoing message
	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()

	message := mq.nextMessage
	mq.nextMessage = nil

	// If the message is too big, split off as much as will fit into a message
	// and save the remainder for the next invocation
	if message != nil && message.Size() > mq.maxMessageSize {
		outgoing, remaining := message.SplitByWantlistSize(mq.maxMessageSize)
		message = outgoing
		mq.nextMessage = remaining
	}
	return message, mq.nextMessage != nil
}

func (mq *MessageQueue) sendMessage() {
	message, work := mq.extractOutgoingMessage()
	if message == nil || message.Empty() {
		return
	}

	// If the message was too big and had to be split, schedule another
	// iteration after this one
	if work {
		defer mq.signalWorkReady()
	}

	// entries := message.Wantlist()
	// for _, e := range entries {
	// 	if e.Cancel {
	// 		if e.WantType == pb.Message_Wantlist_Have {
	// 			log.Debugf("send %s->%s: cancel-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
	// 		} else {
	// 			log.Debugf("send %s->%s: cancel-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
	// 		}
	// 	} else {
	// 		if e.WantType == pb.Message_Wantlist_Have {
	// 			log.Debugf("send %s->%s: want-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
	// 		} else {
	// 			log.Debugf("send %s->%s: want-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
	// 		}
	// 	}
	// }

	err := mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		return
	}

	for i := 0; i < maxRetries; i++ { // try to send this message until we fail.
		if mq.attemptSendAndRecovery(message) {
			return
		}
	}
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
	case <-time.After(time.Millisecond * 100):
		// wait 100ms in case disconnect notifications are still propogating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		// TODO(why): what do we do now?
		// I think the *right* answer is to probably put the message we're
		// trying to send back, and then return to waiting for new work or
		// a disconnect.
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

func (mq *MessageQueue) signalWorkReady() {
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}
