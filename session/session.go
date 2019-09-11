package session

import (
	"context"
	"sync"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	bsgetter "github.com/ipfs/go-bitswap/getter"
	notifications "github.com/ipfs/go-bitswap/notifications"
	bspbkr "github.com/ipfs/go-bitswap/peerbroker"
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
	BroadcastWantHaves(context.Context, uint64, []cid.Cid)
	// WantBlocks(context.Context, []cid.Cid, []cid.Cid, bool, []peer.ID, uint64)
	// CancelWants(context.Context, []cid.Cid, []peer.ID, uint64)
	PeerCanSendWants(peer.ID, []cid.Cid) []cid.Cid
	PeersCanSendWantBlock(cid.Cid, []peer.ID) []peer.ID
	RemoveSession(context.Context, uint64)
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
	keys []cid.Cid
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

	sw    sessionWants
	pb    *bspbkr.PeerBroker
	peers *peer.Set

	latencyTrkr latencyTracker

	// channels
	incoming      chan op
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	idleTick            *time.Timer
	periodicSearchTimer *time.Timer
	baseTickDelay       time.Duration
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
	pb *bspbkr.PeerBroker,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D) *Session {
	s := &Session{
		sw:                  newSessionWants(bpm, wm),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		wm:                  wm,
		pm:                  pm,
		srs:                 srs,
		incoming:            make(chan op, 16),
		pb:                  pb,
		peers:               peer.NewSet(),
		latencyTrkr:         latencyTracker{},
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

func (s *Session) ID() uint64 {
	return s.id
}

func (s *Session) FilterWanted(ks []cid.Cid) []cid.Cid {
	return s.sw.FilterWanted(ks)
}

// ReceiveFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveFrom(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	interestedKs := s.sw.FilterInteresting(ks)

	// s.logReceiveFrom(from, interestedKs, haves, dontHaves)

	// Add any newly discovered peers that have blocks we're interested in to
	// the peer set
	interestedHaves := s.sw.FilterInteresting(haves)
	if len(interestedKs) > 0 || len(interestedHaves) > 0 && !s.peers.Contains(from) {
		s.peers.Add(from)
		log.Infof("Ses%d: Added peer %s to session: %d peers\n", s.id, from, s.peers.Size())
	}

	// Record response timing only if the blocks came from the network
	// (blocks can also be received from the local node)
	if len(interestedKs) > 0 && from != "" {
		s.pm.RecordPeerResponse(from, interestedKs)
	}

	if len(interestedKs) == 0 && len(dontHaves) == 0 {
		return
	}

	wanted, totalLatency := s.sw.ReceiveFrom(from, ks, dontHaves)
	s.latencyTrkr.receiveUpdate(len(wanted), totalLatency)

	if len(wanted) == 0 {
		return
	}

	select {
	case s.incoming <- op{op: opReceive, keys: wanted}:
	case <-s.ctx.Done():
	}
}

func (s *Session) logReceiveFrom(from peer.ID, interestedKs []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	// wantedHaves := s.sw.FilterWanted(haves)
	// wantedDontHaves := s.sw.FilterWanted(dontHaves)
	// log.Infof("Ses%d<-%s: %d blocks, %d haves, %d dont haves\n",
	// 	s.id, from, len(interestedKs), len(wantedHaves), len(wantedDontHaves))
	for _, c := range interestedKs {
		// log.Debugf("Ses%d<-%s: block %s\n", s.id, from, c.String()[2:8])
		log.Warningf("Ses%d<-%s: block %s\n", s.id, from, c.String()[2:8])
	}
	wantedHaves := s.sw.FilterWanted(haves)
	for _, c := range wantedHaves {
		// log.Debugf("Ses%d<-%s: HAVE %s\n", s.id, from, c.String()[2:8])
		log.Warningf("Ses%d<-%s: HAVE %s\n", s.id, from, c.String()[2:8])
	}
	// for _, c := range wantedDontHaves {
	// 	log.Debugf("Ses%d<-%s: DONT_HAVE %s\n", s.id, from, c.String()[2:8])
	// }
}

func (s *Session) MatchWantPeer(ps []peer.ID) *bspbkr.SessionAsk {
	// Check if the session is interested in any of the available peers
	matches := make([]peer.ID, 0, len(ps))
	sessionPeers := s.allPeers()
	for _, p := range ps {
		if sessionPeers.Contains(p) {
			matches = append(matches, p)
		}
	}

	if len(matches) == 0 {
		return nil
	}

	c, wh, p, ph := s.sw.PopNextPending(matches)
	if !c.Defined() {
		return nil
	}

	// Record request for each want sent to peer
	s.pm.RecordPeerRequests([]peer.ID{p}, append(wh, c))
	// Record request for each want-have sent to other peers
	for _, p := range ph {
		s.pm.RecordPeerRequests([]peer.ID{p}, []cid.Cid{c})
	}

	return &bspbkr.SessionAsk{
		Cid:       c,
		WantHaves: wh,
		PeerHaves: ph,
		Peer:      p,
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
				s.handleReceive(oper.keys)
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
		case baseTickDelay := <-s.tickDelayReqs:
			s.baseTickDelay = baseTickDelay
		case <-ctx.Done():
			s.handleShutdown()
			return
		}
	}
}

func (s *Session) handleIdleTick(ctx context.Context) {
	// log.Warningf("\n\n\nSes%d: idle tick\n", s.id)

	live := s.sw.PrepareBroadcast()
	log.Infof("Ses%d: broadcast %d keys\n", s.id, len(live))

	// Broadcast a want-have for the live wants to everyone we're connected to
	s.pm.RecordPeerRequests(nil, live)
	s.wm.BroadcastWantHaves(ctx, s.id, live)

	// do not find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(live) > 0 && (s.consecutiveTicks == 0) {
		s.pm.FindMorePeers(ctx, live[0])
	}
	s.resetIdleTick()

	// If we have live wants
	if s.sw.HasLiveWants() {
		// Inform the potential threshold manager of the idle timeout
		s.sw.IdleTimeout()
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
	// s.wm.WantBlocks(ctx, nil, []cid.Cid{randomWant}, false, nil, s.id)
	s.wm.BroadcastWantHaves(ctx, s.id, []cid.Cid{randomWant})

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

func (s *Session) handleShutdown() {
	s.pb.UnregisterSource(s)
	s.idleTick.Stop()
	// live := s.sw.LiveWants()
	// s.wm.CancelWants(s.ctx, live, nil, s.id)
	s.wm.RemoveSession(s.ctx, s.id)
}

func (s *Session) handleReceive(ks []cid.Cid) {
	s.idleTick.Stop()

	// We've received new wanted blocks, so reset the number of ticks
	// that have occurred since the last new block
	s.consecutiveTicks = 0

	s.pm.RecordCancels(ks)

	s.resetIdleTick()
}

// TODO: Move all peer management into s.pm (SessionPeerManager)
func (s *Session) allPeers() *peer.Set {
	ops := s.pm.GetOptimizedPeers()
	for _, op := range ops {
		s.peers.Add(op.Peer)
	}
	return s.peers
}

func (s *Session) wantBlocks(ctx context.Context, newks []cid.Cid) {
	if len(newks) > 0 {
		s.sw.BlocksRequested(newks)
	}

	// If we have discovered some peers, signal the PeerBroker to ask us for
	// blocks
	if s.allPeers().Size() > 0 {
		// log.Warningf("Ses%d: WantAvailable()\n", s.id)
		s.pb.WantAvailable()
	} else {
		// No peers discovered yet, broadcast some want-haves
		ks := s.sw.GetNextWants(broadcastLiveWantsLimit, nil)
		if len(ks) > 0 {
			log.Infof("Ses%d: No peers - broadcasting %d want HAVE requests\n", s.id, len(ks))
			s.pm.RecordPeerRequests(nil, ks)
			s.wm.BroadcastWantHaves(ctx, s.id, ks)
		}
	}
}

func (s *Session) resetIdleTick() {
	var tickDelay time.Duration
	if !s.latencyTrkr.hasLatency() {
		tickDelay = s.initialSearchDelay
	} else {
		avLat := s.latencyTrkr.averageLatency()
		// log.Warningf("averageLatency %s", avLat)
		tickDelay = s.baseTickDelay + (3 * avLat)
	}
	tickDelay = tickDelay * time.Duration(1+s.consecutiveTicks)
	s.idleTick.Reset(tickDelay)
}

type latencyTracker struct {
	sync.RWMutex
	totalLatency time.Duration
	count        int
}

func (lt *latencyTracker) hasLatency() bool {
	lt.RLock()
	defer lt.RUnlock()

	return lt.totalLatency > 0 && lt.count > 0
}

func (lt *latencyTracker) averageLatency() time.Duration {
	lt.RLock()
	defer lt.RUnlock()

	return lt.totalLatency / time.Duration(lt.count)
}

func (lt *latencyTracker) receiveUpdate(count int, totalLatency time.Duration) {
	lt.Lock()
	defer lt.Unlock()

	lt.totalLatency += totalLatency
	lt.count += count
}
