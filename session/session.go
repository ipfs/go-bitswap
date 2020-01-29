package session

import (
	"context"
	"sync"
	"time"

	// lu "github.com/ipfs/go-bitswap/logutil"
	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	bsgetter "github.com/ipfs/go-bitswap/getter"
	notifications "github.com/ipfs/go-bitswap/notifications"
	bspm "github.com/ipfs/go-bitswap/peermanager"
	bssim "github.com/ipfs/go-bitswap/sessioninterestmanager"
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
	RemoveSession(context.Context, uint64)
}

type PeerManager interface {
	RegisterSession(peer.ID, bspm.Session) bool
	UnregisterSession(uint64)
	SendWants(context.Context, peer.ID, []cid.Cid, []cid.Cid)
}

// PeerManager provides an interface for tracking and optimize peers, and
// requesting more when neccesary.
type SessionPeerManager interface {
	ReceiveFrom(peer.ID, []cid.Cid, []cid.Cid) bool
	Peers() *peer.Set
	FindMorePeers(context.Context, cid.Cid)
	RecordPeerRequests([]peer.ID, []cid.Cid)
	RecordPeerResponse(peer.ID, []cid.Cid)
	RecordCancels([]cid.Cid)
}

type opType int

const (
	opReceive opType = iota
	opWant
	opCancel
	opBroadcast
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
	ctx  context.Context
	wm   WantManager
	sprm SessionPeerManager
	sim  *bssim.SessionInterestManager

	sw  sessionWants
	sws sessionWantSender

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

	self peer.ID
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
func New(ctx context.Context,
	id uint64,
	wm WantManager,
	sprm SessionPeerManager,
	sim *bssim.SessionInterestManager,
	pm PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D,
	self peer.ID) *Session {
	s := &Session{
		sw:                  newSessionWants(),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		wm:                  wm,
		sprm:                sprm,
		sim:                 sim,
		incoming:            make(chan op, 128),
		latencyTrkr:         latencyTracker{},
		notif:               notif,
		uuid:                loggables.Uuid("GetBlockRequest"),
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
		self:                self,
	}
	s.sws = newSessionWantSender(id, pm, bpm, s.onWantsSent, s.onPeersExhausted)

	go s.run(ctx)

	return s
}

func (s *Session) ID() uint64 {
	return s.id
}

// ReceiveFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveFrom(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	interestedRes := s.sim.FilterSessionInterested(s.id, ks, haves, dontHaves)
	ks = interestedRes[0]
	haves = interestedRes[1]
	dontHaves = interestedRes[2]
	// s.logReceiveFrom(from, ks, haves, dontHaves)

	// Add any newly discovered peers that have blocks we're interested in to
	// the peer set
	isNewPeer := s.sprm.ReceiveFrom(from, ks, haves)

	// Record response timing only if the blocks came from the network
	// (blocks can also be received from the local node)
	if len(ks) > 0 && from != "" {
		s.sprm.RecordPeerResponse(from, ks)
	}

	// Update want potential
	s.sws.Update(from, ks, haves, dontHaves, isNewPeer)

	if len(ks) == 0 {
		return
	}

	// Record which blocks have been received and figure out the total latency
	// for fetching the blocks
	wanted, totalLatency := s.sw.BlocksReceived(ks)
	s.latencyTrkr.receiveUpdate(len(wanted), totalLatency)

	if len(wanted) == 0 {
		return
	}

	// Inform the SessionInterestManager that this session is no longer
	// expecting to receive the wanted keys
	s.sim.RemoveSessionWants(s.id, wanted)

	select {
	case s.incoming <- op{op: opReceive, keys: wanted}:
	case <-s.ctx.Done():
	}
}

// func (s *Session) logReceiveFrom(from peer.ID, interestedKs []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
// 	// log.Infof("Ses%d<-%s: %d blocks, %d haves, %d dont haves\n",
// 	// 	s.id, from, len(interestedKs), len(wantedHaves), len(wantedDontHaves))
// 	for _, c := range interestedKs {
// 		log.Warningf("Ses%d %s<-%s: block %s\n", s.id, lu.P(s.self), lu.P(from), lu.C(c))
// 	}
// 	for _, c := range haves {
// 		log.Warningf("Ses%d %s<-%s: HAVE %s\n", s.id, lu.P(s.self), lu.P(from), lu.C(c))
// 	}
// 	for _, c := range dontHaves {
// 		log.Warningf("Ses%d %s<-%s: DONT_HAVE %s\n", s.id, lu.P(s.self), lu.P(from), lu.C(c))
// 	}
// }

func (s *Session) onWantsSent(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	allBlks := append(wantBlocks, wantHaves...)
	s.sw.WantsSent(allBlks)
	s.sprm.RecordPeerRequests([]peer.ID{p}, allBlks)
}

func (s *Session) onPeersExhausted(ks []cid.Cid) {
	select {
	case s.incoming <- op{op: opBroadcast, keys: ks}:
	case <-s.ctx.Done():
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
	go s.sws.Run(ctx)

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
			case opBroadcast:
				s.handleIdleTick(ctx)
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
	live := s.sw.PrepareBroadcast()
	// log.Warningf("\n\n\n\n\nSes%d: broadcast %d keys\n\n\n\n\n", s.id, len(live))
	// s.wm.Trace()
	// log.Infof("Ses%d: broadcast %d keys\n", s.id, len(live))
	log.Warningf("Ses%d: broadcast %d keys", s.id, len(live))

	// Broadcast a want-have for the live wants to everyone we're connected to
	s.sprm.RecordPeerRequests(nil, live)
	s.wm.BroadcastWantHaves(ctx, s.id, live)

	// do not find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(live) > 0 && (s.consecutiveTicks == 0) {
		s.sprm.FindMorePeers(ctx, live[0])
	}
	s.resetIdleTick()

	// If we have live wants
	if s.sw.HasLiveWants() {
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
	s.sprm.FindMorePeers(ctx, randomWant)

	s.wm.BroadcastWantHaves(ctx, s.id, []cid.Cid{randomWant})

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

func (s *Session) handleShutdown() {
	s.idleTick.Stop()
	s.wm.RemoveSession(s.ctx, s.id)
}

func (s *Session) handleReceive(ks []cid.Cid) {
	s.idleTick.Stop()

	// We've received new wanted blocks, so reset the number of ticks
	// that have occurred since the last new block
	s.consecutiveTicks = 0

	s.sprm.RecordCancels(ks)

	s.resetIdleTick()
}

func (s *Session) wantBlocks(ctx context.Context, newks []cid.Cid) {
	if len(newks) > 0 {
		s.sim.RecordSessionInterest(s.id, newks)
		s.sw.BlocksRequested(newks)
		s.sws.Add(newks)
	}

	// If we have discovered peers already, the SessionPotentialManager will
	// send wants to them
	if s.sprm.Peers().Size() > 0 {
		return
	}

	// No peers discovered yet, broadcast some want-haves
	ks := s.sw.GetNextWants(broadcastLiveWantsLimit)
	if len(ks) > 0 {
		log.Infof("Ses%d: No peers - broadcasting %d want HAVE requests\n", s.id, len(ks))
		s.sprm.RecordPeerRequests(nil, ks)
		s.wm.BroadcastWantHaves(ctx, s.id, ks)
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
