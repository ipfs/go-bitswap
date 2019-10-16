// Package decision implements the decision engine for the bitswap service.
package decision

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	lu "github.com/ipfs/go-bitswap/logutil"
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	wl "github.com/ipfs/go-bitswap/wantlist"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
	process "github.com/jbenet/goprocess"
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
	// maxMessageSize = 512 * 1024
	maxMessageSize = 1024 * 1024
	// tagPrefix is the tag given to peers associated an engine
	tagPrefix = "bs-engine-%s"

	// tagWeight is the default weight for peers associated with an engine
	tagWeight = 5

	// maxBlockSizeReplaceHasWithBlock is the maximum size of the block in
	// bytes up to which we will replace a want-have with a want-block
	// maxBlockSizeReplaceHasWithBlock = 1024

	taskWorkerCount = 8

	blockstoreWorkerCount = 128
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

	bsm *blockstoreManager

	peerTagger PeerTagger

	tag  string
	lock sync.RWMutex // protects the fields immediatly below
	// ledgerMap lists Ledgers by their Partner key.
	ledgerMap map[peer.ID]*ledger

	ticker *time.Ticker

	taskWorkerLock  sync.Mutex
	taskWorkerCount int

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
		ledgerMap:                       make(map[peer.ID]*ledger),
		bsm:                             newBlockstoreManager(ctx, bs, blockstoreWorkerCount),
		peerTagger:                      peerTagger,
		outbox:                          make(chan (<-chan *Envelope), outboxChanBuffer),
		workSignal:                      make(chan struct{}, 1),
		ticker:                          time.NewTicker(time.Millisecond * 100),
		maxBlockSizeReplaceHasWithBlock: maxReplaceSize,
		taskWorkerCount:                 taskWorkerCount,
		self:                            self,
	}
	e.tag = fmt.Sprintf(tagPrefix, uuid.New().String())
	e.peerRequestQueue = peertaskqueue.New(
		peertaskqueue.OnPeerAddedHook(e.onPeerAdded),
		peertaskqueue.OnPeerRemovedHook(e.onPeerRemoved),
		peertaskqueue.IgnoreFreezing(true))
	return e
}

func (e *Engine) StartWorkers(ctx context.Context, px process.Process) {
	// Start up blockstore manager
	e.bsm.start(ctx, px)

	// Start up workers to handle requests from other nodes for the data on this node
	for i := 0; i < e.taskWorkerCount; i++ {
		i := i
		px.Go(func(px process.Process) {
			e.taskWorker(ctx, i)
		})
	}
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

func (e *Engine) taskWorker(ctx context.Context, id int) {
	defer e.taskWorkerExit()
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

func (e *Engine) taskWorkerExit() {
	e.taskWorkerLock.Lock()
	defer e.taskWorkerLock.Unlock()

	e.taskWorkerCount--
	if e.taskWorkerCount == 0 {
		close(e.outbox)
	}
}

// nextEnvelope runs in the taskWorker goroutine. Returns an error if the
// context is cancelled before the next Envelope can be created.
func (e *Engine) nextEnvelope(ctx context.Context) (*Envelope, error) {

	// log.Warningf("  nextEnvelope() %s\n", lu.P(e.self))

	for {
		// Pop some tasks off the request queue (up to the maximum message size)
		p, nextTasks := e.peerRequestQueue.PopTasks("", maxMessageSize)
		for len(nextTasks) == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.workSignal:
				p, nextTasks = e.peerRequestQueue.PopTasks(p, maxMessageSize)
				// log.Warningf("  %s got work signal (%d tasks)", lu.P(e.self), len(nextTasks))
			case <-e.ticker.C:
				e.peerRequestQueue.ThawRound()
				p, nextTasks = e.peerRequestQueue.PopTasks(p, maxMessageSize)
				// log.Warningf("  %s tick: thawing round (%d tasks)", lu.P(e.self), len(nextTasks))
			}
		}
		blkCount := 0
		presenceCount := 0

		var msgTasks []peertask.Task
		msg := bsmsg.New(true)
		for len(nextTasks) > 0 {
			// log.Warningf("  %s got %d tasks", lu.P(e.self), len(nextTasks))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			msgTasks = append(msgTasks, nextTasks...)

			// Add DONT_HAVEs to the message
			for _, c := range e.filterDontHaves(nextTasks) {
				presenceCount++
				// log.Warningf("  make evlp %s->%s DONT_HAVE %s", lu.P(e.self), lu.P(p), lu.C(c))
				msg.AddDontHave(c)
			}

			// Add HAVEs to the message
			for _, c := range e.filterWantHaves(nextTasks) {
				presenceCount++
				// log.Warningf("  make evlp %s->%s HAVE %s", lu.P(e.self), lu.P(p), lu.C(c))
				msg.AddHave(c)
			}

			// Get requested blocks
			blockTasks := e.filterWantBlocks(nextTasks)
			blockCids := cid.NewSet()
			for _, t := range blockTasks {
				blockCids.Add(t.Identifier.(cid.Cid))
			}
			blks := e.bsm.getBlocks(blockCids.Keys())

			for _, t := range blockTasks {
				c := t.Identifier.(cid.Cid)
				blk := blks[c]
				blkCount++
				// If the block was not found (it has been removed)
				if blk == nil {
					// If the client requested DONT_HAVE, add it to the message
					if t.SendDontHave {
						// log.Warningf("  make evlp %s->%s DONT_HAVE (expected block) %s", lu.P(e.self), lu.P(p), lu.C(c))
						msg.AddDontHave(c)
					}
				} else {
					// Add the block to the message
					// log.Warningf("  make evlp %s->%s block: %s (%d bytes)", lu.P(e.self), lu.P(p), lu.C(c), len(blk.RawData()))
					msg.AddBlock(blk)
				}
			}

			// Ask the request queue for as much data as will fit into the
			// remaining space in the message
			maxTasksSize := maxMessageSize - msg.Size()
			_, nextTasks = e.peerRequestQueue.PopTasks(p, maxTasksSize)
			// log.Warningf("  asked rq %s->%s for %d bytes, got %d tasks", lu.P(e.self), lu.P(p), maxTasksSize, len(nextTasks))
		}

		if msg.Empty() {
			e.peerRequestQueue.TasksDone(p, msgTasks...)
			continue
		}

		// log.Warningf("  sending message %s->%s (%d blks / %d presences / %d bytes)\n", lu.P(e.self), lu.P(p), blkCount, presenceCount, msg.Size())
		return &Envelope{
			Peer:    p,
			Message: msg,
			Sent: func() {
				e.peerRequestQueue.TasksDone(p, msgTasks...)
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

func (e *Engine) filterDontHaves(tasks []peertask.Task) []cid.Cid {
	var ks []cid.Cid
	for _, t := range tasks {
		if t.BlockSize == 0 {
			ks = append(ks, t.Identifier.(cid.Cid))
		}
	}
	return ks
}

func (e *Engine) filterWantHaves(tasks []peertask.Task) []cid.Cid {
	var ks []cid.Cid
	for _, t := range tasks {
		if t.BlockSize > 0 && !t.IsWantBlock {
			ks = append(ks, t.Identifier.(cid.Cid))
		}
	}
	return ks
}

func (e *Engine) filterWantBlocks(tasks []peertask.Task) []peertask.Task {
	var res []peertask.Task
	for _, t := range tasks {
		if t.BlockSize > 0 && t.IsWantBlock {
			res = append(res, t)
		}
	}
	return res
}

// Outbox returns a channel of one-time use Envelope channels.
func (e *Engine) Outbox() <-chan (<-chan *Envelope) {
	return e.outbox
}

// Peers returns a slice of Peers with whom the local node has active sessions.
func (e *Engine) Peers() []peer.ID {
	e.lock.RLock()
	defer e.lock.RUnlock()

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
		// log.Warningf("engine-%s received message from %s with %d entries\n", lu.P(e.self), lu.P(p), len(entries))
		for _, et := range entries {
			if !et.Cancel {
				if et.WantType == pb.Message_Wantlist_Have {
					log.Debugf("  recv %s<-%s: want-have %s\n", lu.P(e.self), lu.P(p), lu.C(et.Cid))
					// log.Warningf("  recv %s<-%s: want-have %s\n", lu.P(e.self), lu.P(p), lu.C(et.Cid))
				} else {
					log.Debugf("  recv %s<-%s: want-block %s\n", lu.P(e.self), lu.P(p), lu.C(et.Cid))
					// log.Warningf("  recv %s<-%s: want-block %s\n", lu.P(e.self), lu.P(p), lu.C(et.Cid))
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

	// Get block sizes
	wants, cancels := e.splitWantsCancels(entries)
	wantKs := cid.NewSet()
	for _, entry := range wants {
		wantKs.Add(entry.Cid)
	}
	blockSizes := e.bsm.getBlockSizes(wantKs.Keys())

	// Get the ledger for the peer
	l := e.findOrCreate(p)

	// t := time.Now()
	// defer func() { log.Warningf("time to MessageReceived(): %s", time.Since(t)) }()

	// If the peer is sending a full wantlist, replace the ledger's wantlist
	if m.Full() {
		l.lk.Lock()
		l.wantList = wl.New()
		l.lk.Unlock()
	}

	var activeEntries []peertask.Task

	// Remove cancelled blocks from the queue
	if len(cancels) > 0 {
		l.lk.Lock()
		for _, entry := range cancels {
			// log.Warningf("%s<-%s cancel %s", lu.P(e.self), lu.P(p), lu.C(entry.Cid))
			if l.CancelWant(entry.Cid) {
				e.peerRequestQueue.Remove(entry.Cid, p)
			}
		}
		l.lk.Unlock()
	}

	// Add each want-have / want-block to the ledger (we do this separately
	// from the for loop below so as to keep the lock for a shorter time)
	if len(wants) > 0 {
		l.lk.Lock()
		for _, entry := range wants {
			l.Wants(entry.Cid, entry.Priority, entry.WantType)
		}
		l.lk.Unlock()
	}

	// For each want-have / want-block
	for _, entry := range wants {
		c := entry.Cid

		// If the block was not found
		if blockSizes[c] == 0 {
			// Only add the task to the queue if the requester wants a DONT_HAVE
			if entry.SendDontHave {
				newWorkExists = true
				isWantBlock := false
				if entry.WantType == pb.Message_Wantlist_Block {
					isWantBlock = true
				}

				// if isWantBlock {
				// 	log.Warningf("  put rq %s->%s %s as want-block (not found)\n", lu.P(e.self), lu.P(p), lu.C(entry.Cid))
				// } else {
				// 	log.Warningf("  put rq %s->%s %s as want-have (not found)\n", lu.P(e.self), lu.P(p), lu.C(entry.Cid))
				// }

				activeEntries = append(activeEntries, peertask.Task{
					Identifier:   c,
					Priority:     entry.Priority,
					EntrySize:    e.getBlockPresenceSize(c),
					BlockSize:    0,
					IsWantBlock:  isWantBlock,
					SendDontHave: entry.SendDontHave,
				})
			}
			// log.Warningf("  not putting rq %s->%s %s (not found, SendDontHave false)\n", lu.P(e.self), lu.P(p), lu.C(entry.Cid))
		} else {
			// The block was found, add it to the queue
			newWorkExists = true

			blockSize := blockSizes[c]
			isWantBlock := e.sendAsBlock(entry.WantType, blockSize)

			// if isWantBlock {
			// 	log.Warningf("  put rq %s->%s %s as want-block (%d bytes)\n", lu.P(e.self), lu.P(p), lu.C(entry.Cid), blockSize)
			// } else {
			// 	log.Warningf("  put rq %s->%s %s as want-have (%d bytes)\n", lu.P(e.self), lu.P(p), lu.C(entry.Cid), blockSize)
			// }

			entrySize := blockSize
			if !isWantBlock {
				entrySize = e.getBlockPresenceSize(c)
			}
			activeEntries = append(activeEntries, peertask.Task{
				Identifier:   c,
				Priority:     entry.Priority,
				EntrySize:    entrySize,
				BlockSize:    blockSize,
				IsWantBlock:  isWantBlock,
				SendDontHave: entry.SendDontHave,
			})
		}
	}

	// Push entries onto the request queue as a new group
	if len(activeEntries) > 0 {
		e.peerRequestQueue.PushTasks(p, activeEntries...)
	}

	// Record how many bytes were received in the ledger
	// TODO: Record wantlist bytes as well?
	blks := m.Blocks()
	if len(blks) > 0 {
		l.lk.Lock()
		for _, block := range blks {
			log.Debugf("got block %s %d bytes", block, len(block.RawData()))
			l.ReceivedBytes(len(block.RawData()))
		}
		l.lk.Unlock()
	}
}

// Get the size of a HAVE / HAVE_NOT entry
func (e *Engine) getBlockPresenceSize(c cid.Cid) int {
	return bsmsg.BlockPresenceSize(c)
}

func (e *Engine) splitWantsCancels(es []bsmsg.Entry) ([]bsmsg.Entry, []bsmsg.Entry) {
	var wants []bsmsg.Entry
	var cancels []bsmsg.Entry
	for _, et := range es {
		if et.Cancel {
			cancels = append(cancels, et)
		} else {
			wants = append(wants, et)
		}
	}
	return wants, cancels
}

func (e *Engine) filterCancels(es []bsmsg.Entry) []bsmsg.Entry {
	var res []bsmsg.Entry
	for _, e := range es {
		if e.Cancel {
			res = append(es, e)
		}
	}
	return res
}

// ReceiveFrom is called when new blocks are received and added to a block store,
// meaning there may be peers who want those blocks, so we should send the blocks
// to them.
func (e *Engine) ReceiveFrom(from peer.ID, blks []blocks.Block, haves []cid.Cid) {
	e.removeReceivedWants(from, blks, haves)

	if len(blks) == 0 {
		return
	}

	blockSizes := make(map[cid.Cid]int)
	for _, blk := range blks {
		blockSizes[blk.Cid()] = len(blk.RawData())
	}

	work := false
	e.lock.RLock()
	for _, l := range e.ledgerMap {
		l.lk.RLock()

		for _, b := range blks {
			k := b.Cid()

			if entry, ok := l.WantListContains(k); ok {
				work = true

				blockSize := blockSizes[k]
				isWantBlock := e.sendAsBlock(entry.WantType, blockSize)

				// if isWantBlock {
				// 	log.Warningf("  add-block put rq %s->%s %s as want-block (%d bytes)\n", lu.P(e.self), lu.P(l.Partner), lu.C(k), blockSize)
				// } else {
				// 	log.Warningf("  add-block put rq %s->%s %s as want-have (%d bytes)\n", lu.P(e.self), lu.P(l.Partner), lu.C(k), blockSize)
				// }

				entrySize := blockSize
				if !isWantBlock {
					entrySize = e.getBlockPresenceSize(k)
				}

				e.peerRequestQueue.PushTasks(l.Partner, peertask.Task{
					Identifier:   entry.Cid,
					Priority:     entry.Priority,
					EntrySize:    entrySize,
					BlockSize:    blockSize,
					IsWantBlock:  isWantBlock,
					SendDontHave: false,
				})
			}
		}
		l.lk.RUnlock()
	}
	e.lock.RUnlock()

	if work {
		e.signalNewWork()
	}
}

// For each block or HAVE that the remote peer sends us we can remove the
// correspoding want from their ledger
func (e *Engine) removeReceivedWants(from peer.ID, blks []blocks.Block, haves []cid.Cid) {
	if len(blks) == 0 && len(haves) == 0 {
		return
	}

	e.lock.RLock()
	l, ok := e.ledgerMap[from]
	e.lock.RUnlock()

	if !ok {
		return
	}

	l.lk.Lock()
	defer l.lk.Unlock()

	for _, b := range blks {
		c := b.Cid()
		if l.CancelWant(c) {
			// log.Warningf("%s: %s add-block rcvd block cancels %s", lu.P(e.self), lu.P(from), lu.C(c))
			e.peerRequestQueue.Remove(c, from)
		}
	}
	for _, c := range haves {
		if l.CancelWant(c) {
			// log.Warningf("%s: %s add-block rcvd HAVE cancels %s", lu.P(e.self), lu.P(from), lu.C(c))
			e.peerRequestQueue.Remove(c, from)
		}
	}
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
		l.wantList.Remove(block.Cid(), pb.Message_Wantlist_Block)
		// TODO: I think this Remove() will cause the peerRequestQueue to
		// freeze for a while (because it thinks Remove() is called for Cancel)
		// Do we need to call this?
		// e.peerRequestQueue.Remove(block.Cid(), p)
	}

	for _, bp := range m.BlockPresences() {
		// TODO: record block presence bytes as well?
		// l.SentBytes(?)
		if bp.Type == pb.Message_Have {
			c, err := cid.Cast(bp.Cid)
			if err != nil {
				panic(err)
			}
			l.wantList.Remove(c, pb.Message_Wantlist_Have)
		}
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

func (e *Engine) sendAsBlock(wantType pb.Message_Wantlist_WantType, blockSize int) bool {
	isWantBlock := wantType == pb.Message_Wantlist_Block
	return isWantBlock || blockSize <= e.maxBlockSizeReplaceHasWithBlock
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
	// Take a read lock (as it's less expensive) to check if we have a ledger
	// for the peer
	e.lock.RLock()
	l, ok := e.ledgerMap[p]
	e.lock.RUnlock()
	if ok {
		return l
	}

	// There's no ledger, so take a write lock, then check again and create the
	// ledger if necessary
	e.lock.Lock()
	defer e.lock.Unlock()
	l, ok = e.ledgerMap[p]
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
