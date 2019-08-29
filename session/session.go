package session

import (
	"context"
	"time"

	bsgetter "github.com/ipfs/go-bitswap/getter"
	notifications "github.com/ipfs/go-bitswap/notifications"
	bspb "github.com/ipfs/go-bitswap/peerbroker"
	bssd "github.com/ipfs/go-bitswap/sessiondata"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	loggables "github.com/libp2p/go-libp2p-loggables"
)

var log = logging.Logger("bs:sess")

const (
	broadcastLiveWantsLimit = 64
)

// WantManager is an interface that can be used to request blocks
// from given peers.
type WantManager interface {
	WantBlocks(context.Context, []cid.Cid, []cid.Cid, bool, []peer.ID, uint64)
	CancelWants(context.Context, []cid.Cid, []peer.ID, uint64)
	CancelWantHaves(context.Context, []cid.Cid, []peer.ID, uint64)
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

type opType int

const (
	opReceive opType = iota
	opWant
	opCancel
)

type op struct {
	op   opType
	from peer.ID
	keys []cid.Cid
	haves []cid.Cid
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

	sw sessionWants
	pb  *bspb.PeerBroker
	peers *peer.Set

	// channels
	incoming      chan op
	latencyReqs   chan chan time.Duration
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	idleTick            *time.Timer
	periodicSearchTimer *time.Timer
	baseTickDelay       time.Duration
	latTotal            time.Duration
	fetchcnt            int
	consecutiveTicks    int
	recentWasUnique     []bool
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
	pb *bspb.PeerBroker,
	notif notifications.PubSub,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D) *Session {
	s := &Session{
		sw:                  newSessionWants(),
		latencyReqs:         make(chan chan time.Duration),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		wm:                  wm,
		pm:                  pm,
		srs:                 srs,
		incoming:            make(chan op, 16),
		pb:                  pb,
		peers:               peer.NewSet(),
		notif:               notif,
		uuid:                loggables.Uuid("GetBlockRequest"),
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
	}

	pb.RegisterSource(s)

	go s.run(ctx)

	return s
}

// ReceiveFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveFrom(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	s.sw.BlockInfoReceived(from, haves, dontHaves)

	interestedKs := s.sw.FilterInteresting(ks)
	wantedHaves := s.sw.FilterWanted(haves)
	wantedDontHavesCount := s.sw.CountWanted(dontHaves)
	if len(interestedKs) > 0 || len(wantedHaves) == 0 || wantedDontHavesCount > 0 {
		log.Infof("Ses%d: ReceiveFrom %s: %d / %d blocks, %d / %d haves, %d / %d dont haves\n",
			s.id, from, len(interestedKs), len(ks), len(wantedHaves), len(haves), wantedDontHavesCount, len(dontHaves))
	}

	if len(interestedKs) == 0 && len(wantedHaves) == 0 {
		return
	}

	// Add any newly discovered peers that have blocks we're interested in to
	// the peer set
	size := s.peers.Size()
	s.peers.Add(from)
	if (s.peers.Size() > size) {
		log.Infof("Ses%d: Added peer %s to session: %d peers\n", s.id, from, s.peers.Size())
	}

	select {
	case s.incoming <- op{op: opReceive, from: from, keys: interestedKs, haves: wantedHaves}:
	case <-s.ctx.Done():
	}
}

// IsWanted returns true if this session is waiting to receive the given Cid.
func (s *Session) IsWanted(c cid.Cid) bool {
	return s.sw.IsWanted(c)
}

func (s *Session) MatchWantPeer(ps []peer.ID) *bspb.Want {
	// TODO: make sure PendingCount() and PopNextPending() happen atomically
	// (eg pass this match function as a callback to Pop())

	// Check if the session is interested in any of the available peers
	matches := make([]peer.ID, 0, len(ps))
	// peers := s.pm.GetOptimizedPeers()
	peers := s.peers.Peers()
	for _, i := range peers {
		for _, p := range ps {
			if i == p {
				matches = append(matches, p)
			}
		}
	}

	if len(matches) == 0 {
		return nil
	}

	c, wh, p := s.sw.PopNextPending(matches)
	if !c.Defined() {
		// fmt.Println("Dont have match")
		return nil
	}
	// fmt.Println("Do have match")

	// TODO: do this through event loop
	s.pm.RecordPeerRequests([]peer.ID{p}, []cid.Cid{c})

	return &bspb.Want{
		Cid:  c,
		WantHaves: wh,
		Peer: p,
		Ses:  s.id,
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

	return bsgetter.AsyncGetBlocks(ctx, s.ctx, keys, s.notif,
		func(ctx context.Context, keys []cid.Cid) {
			select {
			case s.incoming <- op{op: opWant, keys: keys}:
			case <-ctx.Done():
			case <-s.ctx.Done():
			}
		},
		func(keys []cid.Cid) {
			select {
			case s.incoming <- op{op: opCancel, keys: keys}:
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
		case oper := <-s.incoming:
			switch oper.op {
			case opReceive:
				s.handleReceive(ctx, oper.from, oper.keys, oper.haves)
			case opWant:
				s.wantBlocks(ctx, oper.keys)
			case opCancel:
				s.sw.CancelPending(oper.keys)
			default:
				panic("unhandled operation")
			}
		case <-s.idleTick.C:
			s.handleIdleTick(ctx)
		case <-s.periodicSearchTimer.C:
			s.handlePeriodicSearch(ctx)
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

func (s *Session) handleIdleTick(ctx context.Context) {
	live := s.sw.PrepareBroadcast()
	log.Infof("Ses%d: Idle tick: broadcast %d keys\n", s.id, len(live))

	// Broadcast these keys to everyone we're connected to
	// TODO: Doesn't really make sense to record requests here if we're not getting blocks back
	// (we're now asking for HAVEs, not blocks)
	s.pm.RecordPeerRequests(nil, live)

	// TODO: When this returns, trigger PeerBroker
	s.wm.WantBlocks(ctx, nil, live, false, nil, s.id)

	// do no find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(live) > 0 && (s.consecutiveTicks == 0) {
		s.pm.FindMorePeers(ctx, live[0])
	}
	s.resetIdleTick()

	if s.sw.HasLiveWants() {
		s.sw.IncrementPotentialThreshold()
		s.consecutiveTicks++
	}
}

func (s *Session) handlePeriodicSearch(ctx context.Context) {
	randomWant := s.sw.RandomLiveWant()
	if !randomWant.Defined() {
		return
	}

	// TODO: come up with a better strategy for determining when to search
	// for new providers for blocks.
	s.pm.FindMorePeers(ctx, randomWant)

	// TODO: When this returns, trigger PeerBroker
	s.wm.WantBlocks(ctx, nil, []cid.Cid{randomWant}, false, nil, s.id)

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

func (s *Session) handleShutdown() {
	s.pb.UnregisterSource(s)
	s.idleTick.Stop()
	live := s.sw.LiveWants()
	s.wm.CancelWants(s.ctx, live, nil, s.id)
}

func (s *Session) handleReceive(ctx context.Context, from peer.ID, keys []cid.Cid, haves []cid.Cid) {
	// Record statistics only if the blocks came from the network
	// (blocks can also be received from the local node)
	if from != "" {
		s.updateReceiveCounters(ctx, from, keys)
	}

	// Update the want list
	wanted, totalLatency := s.sw.BlocksReceived(keys)

	if len(wanted) > 0 {
		// We've received the blocks so we can cancel any outstanding wants for
		// them.
		// Record that the block was was cancelled (for latency tracking)
		s.pm.RecordCancels(wanted)
		// Remove the wants for the blocks from the WantManager.
		// This also sends a Cancel message to any peer we had sent a
		// want-block or want-have.
		s.wm.CancelWants(s.ctx, wanted, nil, s.id)
	}

	if len(haves) > 0 {
		// Remove the want-haves for any received HAVEs.
		s.wm.CancelWantHaves(s.ctx, haves, []peer.ID{from}, s.id)
	}

	if len(wanted) > 0 {
		s.idleTick.Stop()

		// Process the received blocks
		s.processIncoming(ctx, wanted, totalLatency)

		s.resetIdleTick()
	} else if len(haves) > 0 {
		// If we didn't get any blocks, but we did get some HAVEs, we must have
		// discovered at least one peer by now, so signal the PeerBroker to
		// ask us if we have wants
		s.pb.WantAvailable()		
	}
}

func (s *Session) updateReceiveCounters(ctx context.Context, from peer.ID, keys []cid.Cid) {
	maxRecent := 256
	// Record unique vs duplicate blocks
	s.sw.ForEachUniqDup(keys, func() {
		s.srs.RecordUniqueBlock()
		s.recentWasUnique = append(s.recentWasUnique, true)
	}, func() {
		s.srs.RecordDuplicateBlock()
		s.recentWasUnique = append(s.recentWasUnique, false)
	})

	poplen := len(s.recentWasUnique) - maxRecent
	if poplen > 0 {
		s.recentWasUnique = s.recentWasUnique[poplen:]
	}
	// fmt.Println("recentWasUnique", s.recentWasUnique)
	if (len(s.recentWasUnique) > 16) {
		unqCount := 1
		dupCount := 1
		for _, u := range s.recentWasUnique {
			if u {
				unqCount++
			} else {
				dupCount++
			}
		}
		total := unqCount + dupCount
		uniqTotalRatio := float64(unqCount) / float64(total)
		log.Debugf("Ses%d: uniq / total: %d / %d = %f\n", s.id, unqCount, total, uniqTotalRatio)
		if uniqTotalRatio < 0.8 {
			s.sw.DecreasePotentialThreshold()
		}
	}

	// Record response (to be able to time latency)
	if len(keys) > 0 {
		s.pm.RecordPeerResponse(from, keys)
	}
}

func (s *Session) processIncoming(ctx context.Context, ks []cid.Cid, totalLatency time.Duration) {
	// Keep track of the total number of blocks received and total latency
	s.fetchcnt += len(ks)

	log.Infof("Ses%d total received blocks: %d\n", s.id, s.fetchcnt)

	s.latTotal += totalLatency

	// We've received new wanted blocks, so reset the number of ticks
	// that have occurred since the last new block
	s.consecutiveTicks = 0

	s.wantBlocks(ctx, nil)
}

func (s *Session) wantBlocks(ctx context.Context, newks []cid.Cid) {
	if len(newks) > 0 {
		s.sw.BlocksRequested(newks)
	}

	// If we have discovered some peers, signal the PeerBroker to ask us for
	// blocks
	if s.peers.Size() > 0 {
		log.Infof("Ses%d: WantAvailable()\n", s.id)
		s.pb.WantAvailable()
	} else {
		// No peers discovered yet, broadcast some want-haves
		ks := s.sw.GetNextWants(broadcastLiveWantsLimit, nil)
		if len(ks) > 0 {
			log.Infof("Ses%d: No peers - broadcasting %d want HAVE requests\n", s.id, len(ks))
			s.pm.RecordPeerRequests(nil, ks)
			s.wm.WantBlocks(ctx, nil, ks, false, nil, s.id)
		}
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
