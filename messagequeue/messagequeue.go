package messagequeue

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	lu "github.com/ipfs/go-bitswap/logutil"
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")

const (
	defaultRebroadcastInterval = 30 * time.Second
	maxRetries                 = 10
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
	ctx     context.Context
	p       peer.ID
	network MessageNetwork

	outgoingWork chan struct{}
	done         chan struct{}

	// do not touch out of run loop
	// wl                    *wantlist.SessionTrackedWantlist
	nextMessage           bsmsg.BitSwapMessage
	nextMessageLk         sync.RWMutex
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer
	priority              int
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork) *MessageQueue {
	return &MessageQueue{
		ctx: ctx,
		// wl:                  wantlist.NewSessionTrackedWantlist(),
		network:             network,
		p:                   p,
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
			mq.nextMessage.AddEntry(c, mq.priority, pb.Message_Wantlist_Have, sendDontHave)
			mq.priority--
		}
	})
}

func (mq *MessageQueue) AddWants(wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	mq.addMessageEntries(func() {
		sendDontHave := true
		for _, c := range wantHaves {
			mq.nextMessage.AddEntry(c, mq.priority, pb.Message_Wantlist_Have, sendDontHave)
			mq.priority--
		}
		for _, c := range wantBlocks {
			mq.nextMessage.AddEntry(c, mq.priority, pb.Message_Wantlist_Block, sendDontHave)
			mq.priority--
		}
	})
}

func (mq *MessageQueue) AddCancels(cancelKs []cid.Cid) {
	mq.addMessageEntries(func() {
		for _, c := range cancelKs {
			mq.nextMessage.Cancel(c)
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

	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
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

func (mq *MessageQueue) addWantlist() {

	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()

	// TODO: rebroadcast
	// if mq.wl.Len() > 0 {
	// 	if mq.nextMessage == nil {
	// 		mq.nextMessage = bsmsg.New(false)
	// 	}
	// 	for _, e := range mq.wl.Entries() {
	// 		mq.nextMessage.AddEntry(e.Cid, e.Priority, e.WantType, e.SendDontHave)
	// 	}
	// 	select {
	// 	case mq.outgoingWork <- struct{}{}:
	// 	default:
	// 	}
	// }
}

func (mq *MessageQueue) rebroadcastWantlist() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer.Reset(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()

	mq.addWantlist()
}

func (mq *MessageQueue) extractOutgoingMessage() bsmsg.BitSwapMessage {
	// TODO: If necessary, split up outgoing message so it's not too big

	// grab outgoing message
	mq.nextMessageLk.Lock()
	message := mq.nextMessage
	mq.nextMessage = nil
	mq.nextMessageLk.Unlock()
	return message
}

func (mq *MessageQueue) sendMessage() {
	message := mq.extractOutgoingMessage()
	if message == nil || message.Empty() {
		return
	}

	entries := message.Wantlist()
	wblocks := 0
	whaves := 0
	cwblocks := 0
	cwhaves := 0
	for _, e := range entries {
		if e.WantType == pb.Message_Wantlist_Have {
			if e.Cancel {
				cwhaves++
			} else {
				whaves++
			}
		} else {
			if e.Cancel {
				cwblocks++
			} else {
				wblocks++
			}
		}
	}
	detail := ""
	if wblocks > 0 {
		detail += fmt.Sprintf(" %d want-block", wblocks)
	}
	if cwblocks > 0 {
		detail += fmt.Sprintf(" %d cancel-block", cwblocks)
	}
	if whaves > 0 {
		detail += fmt.Sprintf(" %d want-have", whaves)
	}
	if cwhaves > 0 {
		detail += fmt.Sprintf(" %d cancel-have", cwhaves)
	}
	// log.Warningf("send %s->%s with %d entries:%s\n", lu.P(mq.network.Self()), lu.P(mq.p), len(entries), detail)
	// for _, e := range entries {
	// 	log.Debugf("  %s: Cancel? %t / WantHave %t / SendDontHave %t\n", e.Cid.String()[2:8], e.Cancel, e.WantHave, e.SendDontHave)
	// }
	for _, e := range entries {
		if e.Cancel {
			if e.WantType == pb.Message_Wantlist_Have {
				// log.Warningf("send %s->%s: cancel-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
			} else {
				// log.Warningf("send %s->%s: cancel-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
			}
		} else {
			if e.WantType == pb.Message_Wantlist_Have {
				log.Debugf("send %s->%s: want-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
				// log.Warningf("send %s->%s: want-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
			} else {
				log.Debugf("send %s->%s: want-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
				// log.Warningf("send %s->%s: want-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
			}
		}
	}

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
