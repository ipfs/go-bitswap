// Package decision implements the decision engine for the bitswap service.
package decision

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	bsmsg "github.com/ipfs/go-bitswap/message"
	lu "github.com/ipfs/go-bitswap/logutil"
	pb "github.com/ipfs/go-bitswap/message/pb"
	wl "github.com/ipfs/go-bitswap/wantlist"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// TODO consider taking responsibility for other types of requests. For
// example, there could be a |cancelQueue| for all of the cancellation
// messages that need to go out. There could also be a |wantlistQueue| for
// the local peer's wantlists. Alternatively, these could all be bundled
// into a single, intelligent global queue that efficiently
// batches/combines and takes all of these into consideration.
//
// Right now, messages go onto the network for four reasons:
// 1. an initial `sendwantlist` message to a provider of the first key in a
//    request
// 2. a periodic full sweep of `sendwantlist` messages to all providers
// 3. upon receipt of blocks, a `cancel` message to all peers
// 4. draining the priority queue of `blockrequests` from peers
//
// Presently, only `blockrequests` are handled by the decision engine.
// However, there is an opportunity to give it more responsibility! If the
// decision engine is given responsibility for all of the others, it can
// intelligently decide how to combine requests efficiently.
//
// Some examples of what would be possible:
//
// * when sending out the wantlists, include `cancel` requests
// * when handling `blockrequests`, include `sendwantlist` and `cancel` as
//   appropriate
// * when handling `cancel`, if we recently received a wanted block from a
//   peer, include a partial wantlist that contains a few other high priority
//   blocks
//
// In a sense, if we treat the decision engine as a black box, it could do
// whatever it sees fit to produce desired outcomes (get wanted keys
// quickly, maintain good relationships with peers, etc).

var log = logging.Logger("engine")

const (
	// outboxChanBuffer must be 0 to prevent stale messages from being sent
	outboxChanBuffer = 0
	// maxMessageSize is the maximum size of the batched payload
	maxMessageSize = 512 * 1024
	// tagPrefix is the tag given to peers associated an engine
	tagPrefix = "bs-engine-%s"

	// tagWeight is the default weight for peers associated with an engine
	tagWeight = 5

	// maxBlockSizeReplaceHasWithBlock is the maximum size of the block in
	// bytes up to which we will replace a want-have with a want-block
	// maxBlockSizeReplaceHasWithBlock = 1024
)

// Envelope contains a message for a Peer.
type Envelope struct {
	// Peer is the intended recipient.
	Peer peer.ID

	// Message is the payload.
	Message bsmsg.BitSwapMessage

	// A callback to notify the decision queue that the task is complete
	Sent func()
}

// wantInfo is used internally to pass information to the PeerTaskQueue and
// get it out on the other side
type wantInfo struct {
	sendDontHave bool
	wantType     pb.Message_Wantlist_WantType
	size         int
}

func (wi *wantInfo) String() string {
	return fmt.Sprintf("want-%s (%d bytes) sendDontHave: %t", wi.wantType, wi.size, wi.sendDontHave)
}

// PeerTagger covers the methods on the connection manager used by the decision
// engine to tag peers
type PeerTagger interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(p peer.ID, tag string)
}

// Engine manages sending requested blocks to peers.
type Engine struct {
	// peerRequestQueue is a priority queue of requests received from peers.
	// Requests are popped from the queue, packaged up, and placed in the
	// outbox.
	peerRequestQueue *peertaskqueue.PeerTaskQueue

	// FIXME it's a bit odd for the client and the worker to both share memory
	// (both modify the peerRequestQueue) and also to communicate over the
	// workSignal channel. consider sending requests over the channel and
	// allowing the worker to have exclusive access to the peerRequestQueue. In
	// that case, no lock would be required.
	workSignal chan struct{}

	// outbox contains outgoing messages to peers. This is owned by the
	// taskWorker goroutine
	outbox chan (<-chan *Envelope)

	bs bstore.Blockstore

	peerTagger PeerTagger

	tag  string
	lock sync.Mutex // protects the fields immediatly below
	// ledgerMap lists Ledgers by their Partner key.
	ledgerMap map[peer.ID]*ledger

	ticker *time.Ticker

	// TODO: make this an optional argument
	// maxBlockSizeReplaceHasWithBlock is the maximum size of the block in
	// bytes up to which we will replace a want-have with a want-block
	maxBlockSizeReplaceHasWithBlock int

	self peer.ID
}

// NewEngine creates a new block sending engine for the given block store
// func NewEngine(ctx context.Context, bs bstore.Blockstore, peerTagger PeerTagger, self peer.ID) *Engine {
func NewEngine(ctx context.Context, bs bstore.Blockstore, peerTagger PeerTagger, self peer.ID, maxReplaceSize int) *Engine {
	e := &Engine{
		ledgerMap:  make(map[peer.ID]*ledger),
		bs:         bs,
		peerTagger: peerTagger,
		outbox:     make(chan (<-chan *Envelope), outboxChanBuffer),
		workSignal: make(chan struct{}, 1),
		ticker:     time.NewTicker(time.Millisecond * 100),
		maxBlockSizeReplaceHasWithBlock: maxReplaceSize,
		self:       "self",
	}
	e.tag = fmt.Sprintf(tagPrefix, uuid.New().String())
	e.peerRequestQueue = peertaskqueue.New(peertaskqueue.OnPeerAddedHook(e.onPeerAdded), peertaskqueue.OnPeerRemovedHook(e.onPeerRemoved))
	go e.taskWorker(ctx)
	return e
}

func (e *Engine) onPeerAdded(p peer.ID) {
	e.peerTagger.TagPeer(p, e.tag, tagWeight)
}

func (e *Engine) onPeerRemoved(p peer.ID) {
	e.peerTagger.UntagPeer(p, e.tag)
}

// WantlistForPeer returns the currently understood want list for a given peer
func (e *Engine) WantlistForPeer(p peer.ID) (out []wl.Entry) {
	partner := e.findOrCreate(p)
	partner.lk.Lock()
	defer partner.lk.Unlock()
	return partner.wantList.SortedEntries()
}

// LedgerForPeer returns aggregated data about blocks swapped and communication
// with a given peer.
func (e *Engine) LedgerForPeer(p peer.ID) *Receipt {
	ledger := e.findOrCreate(p)

	ledger.lk.Lock()
	defer ledger.lk.Unlock()

	return &Receipt{
		Peer:      ledger.Partner.String(),
		Value:     ledger.Accounting.Value(),
		Sent:      ledger.Accounting.BytesSent,
		Recv:      ledger.Accounting.BytesRecv,
		Exchanged: ledger.ExchangeCount(),
	}
}

func (e *Engine) taskWorker(ctx context.Context) {
	defer close(e.outbox) // because taskWorker uses the channel exclusively
	for {
		oneTimeUse := make(chan *Envelope, 1) // buffer to prevent blocking
		select {
		case <-ctx.Done():
			return
		case e.outbox <- oneTimeUse:
		}
		// receiver is ready for an outoing envelope. let's prepare one. first,
		// we must acquire a task from the PQ...
		envelope, err := e.nextEnvelope(ctx)
		if err != nil {
			close(oneTimeUse)
			return // ctx cancelled
		}
		oneTimeUse <- envelope // buffered. won't block
		close(oneTimeUse)
	}
}

// nextEnvelope runs in the taskWorker goroutine. Returns an error if the
// context is cancelled before the next Envelope can be created.
func (e *Engine) nextEnvelope(ctx context.Context) (*Envelope, error) {
	for {
		nextTask := e.peerRequestQueue.PopBlock()
		for nextTask == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.workSignal:
				nextTask = e.peerRequestQueue.PopBlock()
			case <-e.ticker.C:
				e.peerRequestQueue.ThawRound()
				nextTask = e.peerRequestQueue.PopBlock()
			}
		}

		// with a task in hand, we're ready to prepare the envelope...
		msg := bsmsg.New(true)
		for _, entry := range nextTask.Tasks {
			c := entry.Identifier.(cid.Cid)
			info := entry.Info.(*wantInfo)

			if info.wantType == pb.Message_Wantlist_Have {
				// Check if we have the block
				has, err := e.bs.Has(c)
				if err != nil {
					log.Errorf("tried to execute a task and errored stating block: %s", err)
					continue
				}

				// If we have the block
				if has {
					msg.AddHave(c)
				} else if info.sendDontHave {
					// If we don't have the block, and the remote peer asked for a DONT_HAVE
					msg.AddDontHave(c)
				}
			} else {
				// Fetch the block
				block, err := e.bs.Get(c)
				if err != nil {
					// If we don't have the block, and the requester wanted a
					// DONT_HAVE, add it to the message
					if err == bstore.ErrNotFound {
						if info.sendDontHave {
							msg.AddDontHave(c)
						}
					} else {
						log.Errorf("tried to execute a task and errored fetching block: %s", err)
					}
				} else {
					msg.AddBlock(block)
				}
			}
		}

		if msg.Empty() {
			// If we don't have the block, don't hold that against the peer
			// make sure to update that the task has been 'completed'
			nextTask.Done(nextTask.Tasks)
			continue
		}

		return &Envelope{
			Peer:    nextTask.Target,
			Message: msg,
			Sent: func() {
				nextTask.Done(nextTask.Tasks)
				select {
				case e.workSignal <- struct{}{}:
					// work completing may mean that our queue will provide new
					// work to be done.
				default:
				}
			},
		}, nil
	}
}

// Outbox returns a channel of one-time use Envelope channels.
func (e *Engine) Outbox() <-chan (<-chan *Envelope) {
	return e.outbox
}

// Peers returns a slice of Peers with whom the local node has active sessions.
func (e *Engine) Peers() []peer.ID {
	e.lock.Lock()
	defer e.lock.Unlock()

	response := make([]peer.ID, 0, len(e.ledgerMap))

	for _, ledger := range e.ledgerMap {
		response = append(response, ledger.Partner)
	}
	return response
}

// MessageReceived performs book-keeping. Returns error if passed invalid
// arguments.
func (e *Engine) MessageReceived(p peer.ID, m bsmsg.BitSwapMessage) {
	entries := m.Wantlist()

	if len(entries) > 0 {
		log.Debugf("engine-%s received message from %s with %d entries\n", lu.P(e.self), lu.P(p), len(entries))
		for _, et := range entries {
			if !et.Cancel {
				if et.WantType == pb.Message_Wantlist_Have {
					log.Debugf("  recv %s<-%s: want-have %s\n", lu.P(e.self), lu.P(p), lu.C(et.Cid))
				} else {
					log.Debugf("  recv %s<-%s: want-block %s\n", lu.P(e.self), lu.P(p), lu.C(et.Cid))
				}
			}
		}
	}

	if m.Empty() {
		log.Debugf("received empty message from %s", p)
	}

	newWorkExists := false
	defer func() {
		if newWorkExists {
			e.signalNewWork()
		}
	}()

	// Get the ledger for the peer
	l := e.findOrCreate(p)
	l.lk.Lock()
	defer l.lk.Unlock()

	// If the peer is sending a full wantlist, replace the ledger's wantlist
	if m.Full() {
		l.wantList = wl.New()
	}
	// msgNum := rand.Intn(1024)
	// Process each want
	var msgSize int
	var activeEntries []peertask.Task
	for _, entry := range m.Wantlist() {
		// TODO: remove any want-haves from m.Wantlist() if there is a want-block for the same CID

		// If it's a cancel, remove the want from the ledger and send queue
		if entry.Cancel {
			log.Debugf("%s cancel %s", p, entry.Cid)
			if l.CancelWant(entry.Cid) {
				e.peerRequestQueue.Remove(entry.Cid, p)
			}
		} else {
			// If it's a regular want, add it to the ledger and check if we
			// have the block
			l.Wants(entry.Cid, entry.Priority)
			blockSize, err := e.bs.GetSize(entry.Cid)
			if err != nil {
				if err == bstore.ErrNotFound {
					// TODO: Enqueue these before any blocks and give them highest priority
					if entry.SendDontHave {
						newWorkExists = true
						isBlock := false
						if entry.WantType == pb.Message_Wantlist_Block {
							isBlock = true
						}
						activeEntries = append(activeEntries, peertask.Task{
							Identifier:  entry.Cid,
							Priority:    entry.Priority,
							IsBlock:     isBlock,
							Info: &wantInfo{
								sendDontHave: true,
								wantType:     entry.WantType,
								size:         blockSize,
							},
						})
					}
				} else {
					log.Error(err)
				}
			} else {
				// If the request is a want-have, and the block is small
				// enough, treat the request as a want-block
				wantHave := entry.WantType == pb.Message_Wantlist_Have
				sendBlock := !wantHave || (wantHave && blockSize <= e.maxBlockSizeReplaceHasWithBlock)

				// We have the block.
				// Add entries to the send queue in groups such that the total
				// block size of the group is below the maximum message size.
				newWorkExists = true
				if sendBlock && msgSize+blockSize > maxMessageSize {
					e.peerRequestQueue.PushBlock(p, activeEntries...)
					activeEntries = []peertask.Task{}
					msgSize = 0
				}

				wantType := pb.Message_Wantlist_Have
				isBlock := false
				if sendBlock {
					wantType = pb.Message_Wantlist_Block
					isBlock = true
				}

				if sendBlock {
					// log.Warningf("  engine-%s put %s->queue as want-block\n", lu.P(e.self), lu.C(entry.Cid))
				} else {
					// log.Warningf("  engine-%s put %s->queue as want-have\n", lu.P(e.self), lu.C(entry.Cid))
				}
				activeEntries = append(activeEntries, peertask.Task{
					Identifier:  entry.Cid,
					Priority:    entry.Priority,
					IsBlock:     isBlock,
					Info: &wantInfo{
						sendDontHave: entry.SendDontHave,
						wantType:     wantType,
						size:         blockSize,
					},
				})
				msgSize += blockSize
			}
		}
	}
	// Push any remaining entries onto the send queue as a new group
	if len(activeEntries) > 0 {
		e.peerRequestQueue.PushBlock(p, activeEntries...)
	}

	// Record how many bytes were received in the ledger
	for _, block := range m.Blocks() {
		log.Debugf("got block %s %d bytes", block, len(block.RawData()))
		l.ReceivedBytes(len(block.RawData()))
	}
}

func (e *Engine) addBlocks(ks []cid.Cid) {
	// TODO: send out HAVEs to peers that sent us a want-have for these blocks

	// Get the set of blocks that are wanted by our peers
	wantedKs := cid.NewSet()
	for _, l := range e.ledgerMap {
		l.lk.Lock()
		for _, k := range ks {
			if _, ok := l.WantListContains(k); ok {
				wantedKs.Add(k)
			}
		}
		l.lk.Unlock()
	}

	if wantedKs.Len() == 0 {
		return
	}

	// Get the size of each wanted block
	blockSizes := make(map[cid.Cid]int)
	for _, k := range wantedKs.Keys() {
		blockSize, err := e.bs.GetSize(k)
		if err != nil {
			if err != bstore.ErrNotFound {
				log.Warningf("Could not get size of %s from blockstore: %s", k, err)
			}
		} else {
			blockSizes[k] = blockSize
		}
	}

	// Add any wanted blocks to the request queue for each peer
	work := false
	for _, l := range e.ledgerMap {
		for _, k := range wantedKs.Keys() {
			if entry, ok := l.WantListContains(k); ok {
				work = true
				e.peerRequestQueue.PushBlock(l.Partner, peertask.Task{
					Identifier:  entry.Cid,
					Priority:    entry.Priority,
					IsBlock:     true,
					Info: &wantInfo{
						sendDontHave: false,
						wantType:     pb.Message_Wantlist_Block,
						size:         blockSizes[k],
					},
				})
			}
		}
	}

	if work {
		e.signalNewWork()
	}
}

// AddBlocks is called when new blocks are received and added to a block store,
// meaning there may be peers who want those blocks, so we should send the blocks
// to them.
func (e *Engine) AddBlocks(ks []cid.Cid) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.addBlocks(ks)
}

// TODO add contents of m.WantList() to my local wantlist? NB: could introduce
// race conditions where I send a message, but MessageSent gets handled after
// MessageReceived. The information in the local wantlist could become
// inconsistent. Would need to ensure that Sends and acknowledgement of the
// send happen atomically

// MessageSent is called when a message has successfully been sent out, to record
// changes.
func (e *Engine) MessageSent(p peer.ID, m bsmsg.BitSwapMessage) {
	l := e.findOrCreate(p)
	l.lk.Lock()
	defer l.lk.Unlock()

	for _, block := range m.Blocks() {
		l.SentBytes(len(block.RawData()))
		l.wantList.Remove(block.Cid())
		// TODO: I think this Remove() will cause the peerRequestQueue to
		// freeze for a while (because it thinks Remove() is called for Cancel)
		// Do we need to call this?
		// e.peerRequestQueue.Remove(block.Cid(), p)
	}
}

// PeerConnected is called when a new peer connects, meaning we should start
// sending blocks.
func (e *Engine) PeerConnected(p peer.ID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	l, ok := e.ledgerMap[p]
	if !ok {
		l = newLedger(p)
		e.ledgerMap[p] = l
	}
	l.lk.Lock()
	defer l.lk.Unlock()
	l.ref++
}

// PeerDisconnected is called when a peer disconnects.
func (e *Engine) PeerDisconnected(p peer.ID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	l, ok := e.ledgerMap[p]
	if !ok {
		return
	}
	l.lk.Lock()
	defer l.lk.Unlock()
	l.ref--
	if l.ref <= 0 {
		delete(e.ledgerMap, p)
	}
}

func (e *Engine) numBytesSentTo(p peer.ID) uint64 {
	// NB not threadsafe
	return e.findOrCreate(p).Accounting.BytesSent
}

func (e *Engine) numBytesReceivedFrom(p peer.ID) uint64 {
	// NB not threadsafe
	return e.findOrCreate(p).Accounting.BytesRecv
}

// ledger lazily instantiates a ledger
func (e *Engine) findOrCreate(p peer.ID) *ledger {
	e.lock.Lock()
	defer e.lock.Unlock()
	l, ok := e.ledgerMap[p]
	if !ok {
		l = newLedger(p)
		e.ledgerMap[p] = l
	}
	return l
}

func (e *Engine) signalNewWork() {
	// Signal task generation to restart (if stopped!)
	select {
	case e.workSignal <- struct{}{}:
	default:
	}
}
