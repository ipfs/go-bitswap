package bitswap

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	notifications "github.com/ipfs/go-bitswap/notifications"

	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	loggables "github.com/libp2p/go-libp2p-loggables"
	peer "github.com/libp2p/go-libp2p-peer"
)

const broadcastLiveWantsLimit = 4
const targetedLiveWantsLimit = 32

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from
type Session struct {
	ctx            context.Context
	tofetch        *cidQueue
	activePeers    map[peer.ID]struct{}
	activePeersArr []peer.ID

	bs           *Bitswap
	incoming     chan blkRecv
	newReqs      chan []cid.Cid
	cancelKeys   chan []cid.Cid
	interestReqs chan interestReq

	// interest is a cache of cids that are of interest to this session
	// this may include blocks we've already received, but still might want
	// to know about the peer that sent it
	interest *lru.Cache

	// liveWants keeps track of all the current requests we have out, and when
	// they were last requested
	liveWants map[cid.Cid]time.Time
	// liveWantsLimit keeps track of how many live wants we should have out at
	// a time this number should start out low, and grow as we gain more
	// certainty on the sources of our data
	liveWantsLimit int

	tick          *time.Timer
	baseTickDelay time.Duration

	latTotal time.Duration
	// fetchcnt is the number of blocks received
	fetchcnt int
	// broadcasts is the number of times we have broadcasted a set of wants
	broadcasts int
	// dupl is the number of peers to send each want to. In most situations
	// this should be 1, but if the session falls back to broadcasting for
	// blocks too often, this value will be increased to compensate
	dupl int

	notif notifications.PubSub

	uuid logging.Loggable

	id  uint64
	tag string
}

// NewSession creates a new bitswap session whose lifetime is bounded by the
// given context
func (bs *Bitswap) NewSession(ctx context.Context) *Session {
	s := &Session{
		activePeers:    make(map[peer.ID]struct{}),
		liveWants:      make(map[cid.Cid]time.Time),
		liveWantsLimit: broadcastLiveWantsLimit,
		newReqs:        make(chan []cid.Cid),
		cancelKeys:     make(chan []cid.Cid),
		tofetch:        newCidQueue(),
		interestReqs:   make(chan interestReq),
		ctx:            ctx,
		bs:             bs,
		incoming:       make(chan blkRecv),
		notif:          notifications.New(),
		uuid:           loggables.Uuid("GetBlockRequest"),
		baseTickDelay:  time.Millisecond * 500,
		id:             bs.getNextSessionID(),
		dupl:           1,
	}

	s.tag = fmt.Sprint("bs-ses-", s.id)

	cache, _ := lru.New(2048)
	s.interest = cache

	bs.sessLk.Lock()
	bs.sessions = append(bs.sessions, s)
	bs.sessLk.Unlock()

	go s.run(ctx)

	return s
}

func (bs *Bitswap) removeSession(s *Session) {
	s.notif.Shutdown()

	live := make([]cid.Cid, 0, len(s.liveWants))
	for c := range s.liveWants {
		live = append(live, c)
	}
	bs.CancelWants(live, s.id)

	bs.sessLk.Lock()
	defer bs.sessLk.Unlock()
	for i := 0; i < len(bs.sessions); i++ {
		if bs.sessions[i] == s {
			bs.sessions[i] = bs.sessions[len(bs.sessions)-1]
			bs.sessions = bs.sessions[:len(bs.sessions)-1]
			return
		}
	}
}

type blkRecv struct {
	from peer.ID
	blk  blocks.Block
}

func (s *Session) receiveBlockFrom(from peer.ID, blk blocks.Block) {
	select {
	case s.incoming <- blkRecv{from: from, blk: blk}:
	case <-s.ctx.Done():
	}
}

type interestReq struct {
	c    cid.Cid
	resp chan bool
}

// TODO: PERF: this is using a channel to guard a map access against race
// conditions. This is definitely much slower than a mutex, though its unclear
// if it will actually induce any noticeable slowness. This is implemented this
// way to avoid adding a more complex set of mutexes around the liveWants map.
// note that in the average case (where this session *is* interested in the
// block we received) this function will not be called, as the cid will likely
// still be in the interest cache.
func (s *Session) isLiveWant(c cid.Cid) bool {
	resp := make(chan bool, 1)
	select {
	case s.interestReqs <- interestReq{
		c:    c,
		resp: resp,
	}:
	case <-s.ctx.Done():
		return false
	}

	select {
	case want := <-resp:
		return want
	case <-s.ctx.Done():
		return false
	}
}

func (s *Session) interestedIn(c cid.Cid) bool {
	return s.interest.Contains(c) || s.isLiveWant(c)
}

const provSearchDelay = time.Second * 10

func (s *Session) addActivePeer(p peer.ID) {
	if s.liveWantsLimit == broadcastLiveWantsLimit {
		s.liveWantsLimit = targetedLiveWantsLimit
	}

	if _, ok := s.activePeers[p]; !ok {
		s.activePeers[p] = struct{}{}
		s.activePeersArr = append(s.activePeersArr, p)

		cmgr := s.bs.network.ConnectionManager()
		cmgr.TagPeer(p, s.tag, 10)
	}
}

func (s *Session) resetTick() {
	if s.latTotal == 0 {
		s.tick.Reset(provSearchDelay)
	} else {
		avLat := s.latTotal / time.Duration(s.fetchcnt)
		s.tick.Reset(s.baseTickDelay + (3 * avLat))
	}
}

func (s *Session) run(ctx context.Context) {
	s.tick = time.NewTimer(provSearchDelay)
	newpeers := make(chan peer.ID, 16)
	for {
		select {
		case blk := <-s.incoming:
			s.tick.Stop()

			if blk.from != "" {
				s.addActivePeer(blk.from)
			}

			s.receiveBlock(ctx, blk.blk)

			s.resetTick()
		case keys := <-s.newReqs:
			for _, k := range keys {
				s.interest.Add(k, nil)
			}
			if len(s.liveWants) < s.liveWantsLimit {
				toadd := s.liveWantsLimit - len(s.liveWants)
				if toadd > len(keys) {
					toadd = len(keys)
				}

				now := keys[:toadd]
				keys = keys[toadd:]

				s.wantBlocks(ctx, now)
			}
			for _, k := range keys {
				s.tofetch.Push(k)
			}
		case keys := <-s.cancelKeys:
			s.cancel(keys)

		case <-s.tick.C:
			live := make([]cid.Cid, 0, len(s.liveWants))
			now := time.Now()
			for c := range s.liveWants {
				live = append(live, c)
				s.liveWants[c] = now
			}

			// Broadcast these keys to everyone we're connected to
			s.broadcasts++
			s.bs.wm.WantBlocks(ctx, live, nil, s.id)

			if s.fetchcnt > 5 {
				brcRat := float64(s.fetchcnt) / float64(s.broadcasts)
				if brcRat < 2 {
					s.dupl++
				}
			}

			if len(live) > 0 {
				go func(k cid.Cid) {
					// TODO: have a task queue setup for this to:
					// - rate limit
					// - manage timeouts
					// - ensure two 'findprovs' calls for the same block don't run concurrently
					// - share peers between sessions based on interest set
					for p := range s.bs.network.FindProvidersAsync(ctx, k, 10) {
						newpeers <- p
					}
				}(live[0])
			}
			s.resetTick()
		case p := <-newpeers:
			s.addActivePeer(p)
		case lwchk := <-s.interestReqs:
			lwchk.resp <- s.cidIsWanted(lwchk.c)
		case <-ctx.Done():
			s.tick.Stop()
			s.bs.removeSession(s)

			cmgr := s.bs.network.ConnectionManager()
			for _, p := range s.activePeersArr {
				cmgr.UntagPeer(p, s.tag)
			}
			return
		}
	}
}

func (s *Session) cidIsWanted(c cid.Cid) bool {
	_, ok := s.liveWants[c]
	if !ok {
		ok = s.tofetch.Has(c)
	}

	return ok
}

func (s *Session) receiveBlock(ctx context.Context, blk blocks.Block) {
	c := blk.Cid()
	if s.cidIsWanted(c) {
		tval, ok := s.liveWants[c]
		if ok {
			s.latTotal += time.Since(tval)
			delete(s.liveWants, c)
		} else {
			s.tofetch.Remove(c)
		}
		s.fetchcnt++
		s.notif.Publish(blk)

		if next := s.tofetch.Pop(); next.Defined() {
			s.wantBlocks(ctx, []cid.Cid{next})
		}
	}
}

func (s *Session) wantBlocks(ctx context.Context, ks []cid.Cid) {
	now := time.Now()
	for _, c := range ks {
		s.liveWants[c] = now
	}
	if len(s.activePeers) == 0 || s.dupl >= len(s.activePeers) {
		s.broadcasts++
		s.bs.wm.WantBlocks(ctx, ks, s.activePeersArr, s.id)
	} else {
		spl := divvy(ks, len(s.activePeersArr), s.dupl)
		for ki, pi := range rand.Perm(len(s.activePeersArr)) {
			s.bs.wm.WantBlocks(ctx, spl[ki], []peer.ID{s.activePeersArr[pi]}, s.id)
		}
	}
}

func divvy(ks []cid.Cid, n, dupl int) [][]cid.Cid {
	out := make([][]cid.Cid, n)
	for l := 0; l < dupl; l++ {
		for i, c := range ks {
			pos := (i + (len(ks) * l)) % n
			out[pos] = append(out[pos], c)
		}
	}
	return out
}

func (s *Session) cancel(keys []cid.Cid) {
	for _, c := range keys {
		s.tofetch.Remove(c)
	}
}

func (s *Session) cancelWants(keys []cid.Cid) {
	select {
	case s.cancelKeys <- keys:
	case <-s.ctx.Done():
	}
}

func (s *Session) fetch(ctx context.Context, keys []cid.Cid) {
	select {
	case s.newReqs <- keys:
	case <-ctx.Done():
	case <-s.ctx.Done():
	}
}

// GetBlocks fetches a set of blocks within the context of this session and
// returns a channel that found blocks will be returned on. No order is
// guaranteed on the returned blocks.
func (s *Session) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	ctx = logging.ContextWithLoggable(ctx, s.uuid)
	return getBlocksImpl(ctx, keys, s.notif, s.fetch, s.cancelWants)
}

// GetBlock fetches a single block
func (s *Session) GetBlock(parent context.Context, k cid.Cid) (blocks.Block, error) {
	return getBlock(parent, k, s.GetBlocks)
}

type cidQueue struct {
	elems []cid.Cid
	eset  *cid.Set
}

func newCidQueue() *cidQueue {
	return &cidQueue{eset: cid.NewSet()}
}

func (cq *cidQueue) Pop() cid.Cid {
	for {
		if len(cq.elems) == 0 {
			return cid.Cid{}
		}

		out := cq.elems[0]
		cq.elems = cq.elems[1:]

		if cq.eset.Has(out) {
			cq.eset.Remove(out)
			return out
		}
	}
}

func (cq *cidQueue) Push(c cid.Cid) {
	if cq.eset.Visit(c) {
		cq.elems = append(cq.elems, c)
	}
}

func (cq *cidQueue) Remove(c cid.Cid) {
	cq.eset.Remove(c)
}

func (cq *cidQueue) Has(c cid.Cid) bool {
	return cq.eset.Has(c)
}

func (cq *cidQueue) Len() int {
	return cq.eset.Len()
}
