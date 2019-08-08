package session

import (
	"context"
	"math/rand"
	"time"

	lru "github.com/hashicorp/golang-lru"
	bsgetter "github.com/ipfs/go-bitswap/getter"
	notifications "github.com/ipfs/go-bitswap/notifications"
	bssd "github.com/ipfs/go-bitswap/sessiondata"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	loggables "github.com/libp2p/go-libp2p-loggables"
)

const (
	broadcastLiveWantsLimit = 4
	targetedLiveWantsLimit  = 32
)

// WantManager is an interface that can be used to request blocks
// from given peers.
type WantManager interface {
	WantBlocks(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64)
	CancelWants(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64)
}

// PeerManager provides an interface for tracking and optimize peers, and
// requesting more when neccesary.
type PeerManager interface {
	FindMorePeers(context.Context, cid.Cid)
	GetOptimizedPeers() []bssd.OptimizedPeer
	RecordPeerRequests([]peer.ID, []cid.Cid)
	RecordPeerResponse(peer.ID, []cid.Cid)
	RecordCancels([]cid.Cid)
}

// RequestSplitter provides an interface for splitting
// a request for Cids up among peers.
type RequestSplitter interface {
	SplitRequest([]bssd.OptimizedPeer, []cid.Cid) []bssd.PartialRequest
	RecordDuplicateBlock()
	RecordUniqueBlock()
}

type interestReq struct {
	c    cid.Cid
	resp chan bool
}

type blksRecv struct {
	from        peer.ID
	blks        []blocks.Block
	dups        []blocks.Block
	fromNetwork bool
}

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
type Session struct {
	// dependencies
	ctx context.Context
	wm  WantManager
	pm  PeerManager
	srs RequestSplitter

	// channels
	incoming      chan blksRecv
	newReqs       chan []cid.Cid
	cancelKeys    chan []cid.Cid
	interestReqs  chan interestReq
	latencyReqs   chan chan time.Duration
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	tofetch             *cidQueue
	interest            *lru.Cache
	pastWants           *cidQueue
	liveWants           map[cid.Cid]time.Time
	idleTick            *time.Timer
	periodicSearchTimer *time.Timer
	baseTickDelay       time.Duration
	latTotal            time.Duration
	fetchcnt            int
	consecutiveTicks    int
	initialSearchDelay  time.Duration
	periodicSearchDelay delay.D
	// identifiers
	notif notifications.PubSub
	uuid  logging.Loggable
	id    uint64
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
func New(ctx context.Context,
	id uint64,
	wm WantManager,
	pm PeerManager,
	srs RequestSplitter,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D) *Session {
	s := &Session{
		liveWants:           make(map[cid.Cid]time.Time),
		newReqs:             make(chan []cid.Cid),
		cancelKeys:          make(chan []cid.Cid),
		tofetch:             newCidQueue(),
		pastWants:           newCidQueue(),
		interestReqs:        make(chan interestReq),
		latencyReqs:         make(chan chan time.Duration),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		wm:                  wm,
		pm:                  pm,
		srs:                 srs,
		incoming:            make(chan blksRecv),
		notif:               notifications.New(),
		uuid:                loggables.Uuid("GetBlockRequest"),
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
	}

	cache, _ := lru.New(2048)
	s.interest = cache

	go s.run(ctx)

	return s
}

// ReceiveBlocksFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveBlocksFrom(from peer.ID, blocks []blocks.Block, dups []blocks.Block, fromNetwork bool) {
	select {
	case s.incoming <- blksRecv{from: from, blks: blocks, dups: dups, fromNetwork: fromNetwork}:
	case <-s.ctx.Done():
	}

	// We've received the blocks so we can cancel any outstanding wants for them
	ks := make([]cid.Cid, 0, len(blocks))
	for _, b := range blocks {
		ks = append(ks, b.Cid())
	}
	s.pm.RecordCancels(ks)
	s.wm.CancelWants(s.ctx, ks, nil, s.id)
}

// InterestedIn returns true if this session is interested in the given Cid.
func (s *Session) InterestedIn(c cid.Cid) bool {
	if s.interest.Contains(c) {
		return true
	}
	// TODO: PERF: this is using a channel to guard a map access against race
	// conditions. This is definitely much slower than a mutex, though its unclear
	// if it will actually induce any noticeable slowness. This is implemented this
	// way to avoid adding a more complex set of mutexes around the liveWants map.
	// note that in the average case (where this session *is* interested in the
	// block we received) this function will not be called, as the cid will likely
	// still be in the interest cache.
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

// GetBlock fetches a single block.
func (s *Session) GetBlock(parent context.Context, k cid.Cid) (blocks.Block, error) {
	return bsgetter.SyncGetBlock(parent, k, s.GetBlocks)
}

// GetBlocks fetches a set of blocks within the context of this session and
// returns a channel that found blocks will be returned on. No order is
// guaranteed on the returned blocks.
func (s *Session) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	ctx = logging.ContextWithLoggable(ctx, s.uuid)
	return bsgetter.AsyncGetBlocks(ctx, keys, s.notif,
		func(ctx context.Context, keys []cid.Cid) {
			select {
			case s.newReqs <- keys:
			case <-ctx.Done():
			case <-s.ctx.Done():
			}
		},
		func(keys []cid.Cid) {
			select {
			case s.cancelKeys <- keys:
			case <-s.ctx.Done():
			}
		},
	)
}

// GetAverageLatency returns the average latency for block requests.
func (s *Session) GetAverageLatency() time.Duration {
	resp := make(chan time.Duration)
	select {
	case s.latencyReqs <- resp:
	case <-s.ctx.Done():
		return -1 * time.Millisecond
	}

	select {
	case latency := <-resp:
		return latency
	case <-s.ctx.Done():
		return -1 * time.Millisecond
	}
}

// SetBaseTickDelay changes the rate at which ticks happen.
func (s *Session) SetBaseTickDelay(baseTickDelay time.Duration) {
	select {
	case s.tickDelayReqs <- baseTickDelay:
	case <-s.ctx.Done():
	}
}

// Session run loop -- everything function below here should not be called
// of this loop
func (s *Session) run(ctx context.Context) {
	s.idleTick = time.NewTimer(s.initialSearchDelay)
	s.periodicSearchTimer = time.NewTimer(s.periodicSearchDelay.NextWaitTime())
	for {
		select {
		case rcv := <-s.incoming:
			if rcv.fromNetwork {
				s.updateReceiveCounters(ctx, rcv)
			}
			s.handleIncomingBlocks(ctx, rcv)
		case keys := <-s.newReqs:
			s.handleNewRequest(ctx, keys)
		case keys := <-s.cancelKeys:
			s.handleCancel(keys)
		case <-s.idleTick.C:
			s.handleIdleTick(ctx)
		case <-s.periodicSearchTimer.C:
			s.handlePeriodicSearch(ctx)
		case lwchk := <-s.interestReqs:
			lwchk.resp <- s.cidIsWanted(lwchk.c)
		case resp := <-s.latencyReqs:
			resp <- s.averageLatency()
		case baseTickDelay := <-s.tickDelayReqs:
			s.baseTickDelay = baseTickDelay
		case <-ctx.Done():
			s.handleShutdown()
			return
		}
	}
}

func (s *Session) handleIncomingBlocks(ctx context.Context, rcv blksRecv) {
	s.idleTick.Stop()

	// Process the received blocks
	s.receiveBlocks(ctx, rcv.blks)

	s.resetIdleTick()
}

func (s *Session) handleNewRequest(ctx context.Context, keys []cid.Cid) {
	for _, k := range keys {
		s.interest.Add(k, nil)
	}
	if toadd := s.wantBudget(); toadd > 0 {
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
}

func (s *Session) handleCancel(keys []cid.Cid) {
	for _, c := range keys {
		s.tofetch.Remove(c)
	}
}

func (s *Session) handleIdleTick(ctx context.Context) {
	live := make([]cid.Cid, 0, len(s.liveWants))
	now := time.Now()
	for c := range s.liveWants {
		live = append(live, c)
		s.liveWants[c] = now
	}

	// Broadcast these keys to everyone we're connected to
	s.pm.RecordPeerRequests(nil, live)
	s.wm.WantBlocks(ctx, live, nil, s.id)

	// do no find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(live) > 0 && (s.consecutiveTicks == 0) {
		s.pm.FindMorePeers(ctx, live[0])
	}
	s.resetIdleTick()

	if len(s.liveWants) > 0 {
		s.consecutiveTicks++
	}
}

func (s *Session) handlePeriodicSearch(ctx context.Context) {
	randomWant := s.randomLiveWant()
	if !randomWant.Defined() {
		return
	}

	// TODO: come up with a better strategy for determining when to search
	// for new providers for blocks.
	s.pm.FindMorePeers(ctx, randomWant)
	s.wm.WantBlocks(ctx, []cid.Cid{randomWant}, nil, s.id)

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

func (s *Session) randomLiveWant() cid.Cid {
	if len(s.liveWants) == 0 {
		return cid.Cid{}
	}
	i := rand.Intn(len(s.liveWants))
	// picking a random live want
	for k := range s.liveWants {
		if i == 0 {
			return k
		}
		i--
	}
	return cid.Cid{}
}
func (s *Session) handleShutdown() {
	s.idleTick.Stop()
	s.notif.Shutdown()

	live := make([]cid.Cid, 0, len(s.liveWants))
	for c := range s.liveWants {
		live = append(live, c)
	}
	s.wm.CancelWants(s.ctx, live, nil, s.id)
}

func (s *Session) cidIsWanted(c cid.Cid) bool {
	_, ok := s.liveWants[c]
	if !ok {
		ok = s.tofetch.Has(c)
	}
	return ok
}

func (s *Session) receiveBlocks(ctx context.Context, blocks []blocks.Block) {
	for _, blk := range blocks {
		c := blk.Cid()
		if s.cidIsWanted(c) {
			// If the block CID was in the live wants queue, remove it
			tval, ok := s.liveWants[c]
			if ok {
				s.latTotal += time.Since(tval)
				delete(s.liveWants, c)
			} else {
				// Otherwise remove it from the tofetch queue, if it was there
				s.tofetch.Remove(c)
			}
			s.fetchcnt++

			// We've received new wanted blocks, so reset the number of ticks
			// that have occurred since the last new block
			s.consecutiveTicks = 0

			s.notif.Publish(blk)

			// Keep track of CIDs we've successfully fetched
			s.pastWants.Push(c)
		}
	}

	// Transfer as many CIDs as possible from the tofetch queue into the
	// live wants queue
	toAdd := s.wantBudget()
	if toAdd > s.tofetch.Len() {
		toAdd = s.tofetch.Len()
	}
	if toAdd > 0 {
		var keys []cid.Cid
		for i := 0; i < toAdd; i++ {
			keys = append(keys, s.tofetch.Pop())
		}
		s.wantBlocks(ctx, keys)
	}
}

func (s *Session) updateReceiveCounters(ctx context.Context, rcv blksRecv) {
	// Inform the request splitter of unique / duplicate blocks
	for _, blk := range append(rcv.blks, rcv.dups...) {
		if s.cidIsWanted(blk.Cid()) {
			s.srs.RecordUniqueBlock()
		} else if s.pastWants.Has(blk.Cid()) {
			s.srs.RecordDuplicateBlock()
		}
	}

	// If this is a response to a targeted request (ie not a broadcast request)
	// then record the peer response
	if rcv.from != "" {
		ksm := make(map[cid.Cid]struct{}, len(rcv.blks)+len(rcv.dups))
		for _, b := range rcv.blks {
			ksm[b.Cid()] = struct{}{}
		}
		for _, b := range rcv.dups {
			if s.pastWants.Has(b.Cid()) {
				ksm[b.Cid()] = struct{}{}
			}
		}

		if len(ksm) > 0 {
			ks := make([]cid.Cid, len(ksm))
			for c, _ := range ksm {
				ks = append(ks, c)
			}
			s.pm.RecordPeerResponse(rcv.from, ks)
		}
	}
}

func (s *Session) wantBlocks(ctx context.Context, ks []cid.Cid) {
	now := time.Now()
	for _, c := range ks {
		s.liveWants[c] = now
	}
	peers := s.pm.GetOptimizedPeers()
	if len(peers) > 0 {
		splitRequests := s.srs.SplitRequest(peers, ks)
		for _, splitRequest := range splitRequests {
			s.pm.RecordPeerRequests(splitRequest.Peers, splitRequest.Keys)
			s.wm.WantBlocks(ctx, splitRequest.Keys, splitRequest.Peers, s.id)
		}
	} else {
		s.pm.RecordPeerRequests(nil, ks)
		s.wm.WantBlocks(ctx, ks, nil, s.id)
	}
}

func (s *Session) averageLatency() time.Duration {
	return s.latTotal / time.Duration(s.fetchcnt)
}

func (s *Session) resetIdleTick() {
	var tickDelay time.Duration
	if s.latTotal == 0 {
		tickDelay = s.initialSearchDelay
	} else {
		avLat := s.averageLatency()
		tickDelay = s.baseTickDelay + (3 * avLat)
	}
	tickDelay = tickDelay * time.Duration(1+s.consecutiveTicks)
	s.idleTick.Reset(tickDelay)
}

func (s *Session) wantBudget() int {
	live := len(s.liveWants)
	var budget int
	if len(s.pm.GetOptimizedPeers()) > 0 {
		budget = targetedLiveWantsLimit - live
	} else {
		budget = broadcastLiveWantsLimit - live
	}
	if budget < 0 {
		budget = 0
	}
	return budget
}
