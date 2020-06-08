package session

import (
	"context"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bsgetter "github.com/ipfs/go-bitswap/internal/getter"
	notifications "github.com/ipfs/go-bitswap/internal/notifications"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	loggables "github.com/libp2p/go-libp2p-loggables"
	"go.uber.org/zap"
)

var log = logging.Logger("bs:sess")
var sflog = log.Desugar()

const (
	broadcastLiveWantsLimit = 64
)

// PeerManager keeps track of which sessions are interested in which peers
// and takes care of sending wants for the sessions
type PeerManager interface {
	// RegisterSession tells the PeerManager that the session is interested
	// in a peer's connection state
	RegisterSession(peer.ID, bspm.Session) bool
	// UnregisterSession tells the PeerManager that the session is no longer
	// interested in a peer's connection state
	UnregisterSession(uint64)
	// SendWants tells the PeerManager to send wants to the given peer
	SendWants(sid uint64, peerId peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid)
	// BroadcastWantHaves sends want-haves to all connected peers (used for
	// session discovery)
	BroadcastWantHaves(sid uint64, whs []cid.Cid)
	// SendCancels tells the PeerManager to send cancels to all peers
	SendCancels(sid uint64, cs []cid.Cid)
}

// SessionManager manages all the sessions
type SessionManager interface {
	// Remove a session (called when the session shuts down)
	RemoveSession(sesid uint64)
}

// SessionPeerManager keeps track of peers in the session
type SessionPeerManager interface {
	// Shutdown the SessionPeerManager
	Shutdown()
	// Adds a peer to the session, returning true if the peer is new
	AddPeer(peer.ID) bool
	// Removes a peer from the session, returning true if the peer existed
	RemovePeer(peer.ID) bool
	// All peers in the session
	Peers() []peer.ID
	// Whether there are any peers in the session
	HasPeers() bool
}

// ProviderFinder is used to find providers for a given key
type ProviderFinder interface {
	// FindProvidersAsync searches for peers that provide the given CID
	FindProvidersAsync(ctx context.Context, k cid.Cid) <-chan peer.ID
}

// opType is the kind of operation that is being processed by the event loop
type opType int

const (
	// Receive blocks
	opReceive opType = iota
	// Want blocks
	opWant
	// Cancel wants
	opCancel
	// Broadcast want-haves
	opBroadcast
	// Wants sent to peers
	opWantsSent
)

type op struct {
	op          opType
	wantRequest *notifications.WantRequest
	p           peer.ID
	keys        []cid.Cid
	haves       []cid.Cid
	dontHaves   []cid.Cid
}

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
type Session struct {
	// dependencies
	ctx            context.Context
	shutdown       func()
	sm             SessionManager
	pm             PeerManager
	providerFinder ProviderFinder

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
	wantRequests        map[*notifications.WantRequest]struct{}
	peersDiscovered     bool

	// identifiers
	wrm  *notifications.WantRequestManager
	uuid logging.Loggable
	id   uint64

	self peer.ID
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
func New(
	ctx context.Context,
	sm SessionManager,
	id uint64,
	sprm SessionPeerManager,
	providerFinder ProviderFinder,
	pm PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	wrm *notifications.WantRequestManager,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D,
	self peer.ID) *Session {

	ctx, cancel := context.WithCancel(ctx)
	s := &Session{
		sw:                  newSessionWants(broadcastLiveWantsLimit),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		shutdown:            cancel,
		sm:                  sm,
		pm:                  pm,
		providerFinder:      providerFinder,
		incoming:            make(chan op, 128),
		latencyTrkr:         latencyTracker{},
		wrm:                 wrm,
		uuid:                loggables.Uuid("GetBlockRequest"),
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
		wantRequests:        make(map[*notifications.WantRequest]struct{}),
		self:                self,
	}
	s.sws = newSessionWantSender(id, pm, sprm, bpm, s.onWantsSent, s.onPeersExhausted)

	go s.run(ctx)

	return s
}

func (s *Session) ID() uint64 {
	return s.id
}

func (s *Session) Shutdown() {
	s.shutdown()
}

// GetBlock fetches a single block.
func (s *Session) GetBlock(parent context.Context, k cid.Cid) (blocks.Block, error) {
	return bsgetter.SyncGetBlock(parent, k, s.GetBlocks)
}

// GetBlocks fetches a set of blocks within the context of this session and
// returns a channel that found blocks will be returned on. No order is
// guaranteed on the returned blocks.
func (s *Session) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	// If there are no keys supplied, just return a closed channel
	if len(keys) == 0 {
		out := make(chan blocks.Block)
		close(out)
		return out, nil
	}

	// Use a WantRequest to listen for incoming messages pertaining to the keys
	var wr *notifications.WantRequest
	var err error
	wr, err = s.wrm.NewWantRequest(keys, func(ks []cid.Cid) {
		s.incoming <- op{
			op:          opCancel,
			keys:        ks,
			wantRequest: wr,
		}
	})
	if err != nil {
		return nil, err
	}

	for _, c := range keys {
		log.Debugw("Bitswap.GetBlockRequest.Start", "cid", c)
	}

	// Add wanted keys to the session
	select {
	case s.incoming <- op{op: opWant, keys: keys, wantRequest: wr}:
	case <-ctx.Done():
	case <-s.ctx.Done():
	}

	// Listen for incoming messages
	go wr.Run(s.ctx, ctx, s.receiveMessage)

	return wr.Out, nil
}

func (s *Session) receiveMessage(msg *notifications.IncomingMessage) {
	// Log the incoming message
	s.logReceiveFrom(msg)

	blks := make([]cid.Cid, 0, len(msg.Blks))
	for _, b := range msg.Blks {
		blks = append(blks, b.Cid())
	}
	s.blockingEnqueue(op{op: opReceive, p: msg.From, keys: blks, haves: msg.Haves, dontHaves: msg.DontHaves})
}

// SetBaseTickDelay changes the rate at which ticks happen.
func (s *Session) SetBaseTickDelay(baseTickDelay time.Duration) {
	select {
	case s.tickDelayReqs <- baseTickDelay:
	case <-s.ctx.Done():
	}
}

// onWantsSent is called when wants are sent to a peer by the session wants sender
func (s *Session) onWantsSent(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	allBlks := append(wantBlocks[:len(wantBlocks):len(wantBlocks)], wantHaves...)
	s.nonBlockingEnqueue(op{op: opWantsSent, keys: allBlks})
}

// onPeersExhausted is called when all available peers have sent DONT_HAVE for
// a set of cids (or all peers become unavailable)
func (s *Session) onPeersExhausted(ks []cid.Cid) {
	s.nonBlockingEnqueue(op{op: opBroadcast, keys: ks})
}

// We don't want to block the sessionWantSender if the incoming channel
// is full. So if we can't immediately send on the incoming channel spin
// it off into a go-routine.
func (s *Session) nonBlockingEnqueue(o op) {
	select {
	case s.incoming <- o:
	default:
		go s.blockingEnqueue(o)
	}
}

func (s *Session) blockingEnqueue(o op) {
	select {
	case s.incoming <- o:
	case <-s.ctx.Done():
	}
}

// Session run loop -- everything in this function should not be called
// outside of this loop
func (s *Session) run(ctx context.Context) {
	defer s.handleShutdown()

	go s.sws.Run()

	s.idleTick = time.NewTimer(s.initialSearchDelay)
	s.periodicSearchTimer = time.NewTimer(s.periodicSearchDelay.NextWaitTime())

	for ctx.Err() == nil {
		select {

		// Session event received
		case oper := <-s.incoming:
			switch oper.op {

			// Request for new wants
			case opWant:
				// Save a reference to the WantRequest
				s.wantRequests[oper.wantRequest] = struct{}{}
				// Request the blocks
				s.sws.Add(oper.keys)
				// Register that the client wants the blocks
				s.wantBlocks(oper.keys)

			// Message received
			case opReceive:
				// Inform the session want sender that a message was received
				s.sws.Update(oper.p, oper.keys, oper.haves, oper.dontHaves)
				// Received blocks
				s.handleReceive(oper.keys, oper.haves)

			// Want request cancelled
			case opCancel:
				// Send cancels
				s.sws.Cancel(oper.keys)
				// Record that wants were cancelled
				s.sw.CancelPending(oper.keys)
				// Clean up WantRequest
				delete(s.wantRequests, oper.wantRequest)

			// sesssionWantSender sent wants
			case opWantsSent:
				// Wants were sent to a peer
				s.sw.WantsSent(oper.keys)

			// sessionWantSender requests broadcast
			case opBroadcast:
				// Broadcast want-haves to all peers
				s.broadcast(ctx, oper.keys)

			default:
				panic("unhandled operation")
			}

		case <-s.idleTick.C:
			// The session hasn't received blocks for a while, broadcast
			s.broadcast(ctx, nil)

		case <-s.periodicSearchTimer.C:
			// Periodically search for a random live want
			s.handlePeriodicSearch(ctx)

		case baseTickDelay := <-s.tickDelayReqs:
			// Set the base tick delay
			s.baseTickDelay = baseTickDelay

		case <-ctx.Done():
			return
		}
	}
}

// handleShutdown is called when the session shuts down
func (s *Session) handleShutdown() {
	// Stop the timers
	s.idleTick.Stop()
	s.periodicSearchTimer.Stop()
	// Drain any remaining cancels from the queue
	s.drainCancels()
	// Shut down the sessionWantSender (blocks till cancels have been processed)
	s.sws.Shutdown()
	// Signal to the SessionManager that the session has been shutdown
	// and can be cleaned up
	s.sm.RemoveSession(s.id)
}

// drainCancels receives on the incoming channel until a cancel request for
// each WantRequest has been received and passed to the sessionWantSender
func (s *Session) drainCancels() {
	for len(s.wantRequests) > 0 {
		oper := <-s.incoming
		switch oper.op {
		case opCancel:
			// Send cancels
			s.sws.Cancel(oper.keys)
			// Clean up WantRequest
			delete(s.wantRequests, oper.wantRequest)
		case opReceive:
			// Inform the session want sender that a message was received
			// (so that it can cancel the corresponding block)
			s.sws.Update(oper.p, oper.keys, nil, nil)
		}
	}
}

// Called when the session hasn't received any blocks for some time, or when
// all peers in the session have sent DONT_HAVE for a particular set of CIDs.
// Send want-haves to all connected peers, and search for new peers with the CID.
func (s *Session) broadcast(ctx context.Context, wants []cid.Cid) {
	// If this broadcast is because of an idle timeout (we haven't received
	// any blocks for a while) then broadcast all pending wants
	if wants == nil {
		wants = s.sw.PrepareBroadcast()
	}

	// Broadcast a want-have for the live wants to everyone we're connected to
	s.broadcastWantHaves(wants)

	// do not find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(wants) > 0 && (s.consecutiveTicks == 0) {
		// Search for providers who have the first want in the list.
		// Typically if the provider has the first block they will have
		// the rest of the blocks also.
		log.Debugw("FindMorePeers", "session", s.id, "cid", wants[0], "pending", len(wants))
		s.findMorePeers(ctx, wants[0])
	}
	s.resetIdleTick()

	// If we have live wants record a consecutive tick
	if s.sw.HasLiveWants() {
		s.consecutiveTicks++
	}
}

// handlePeriodicSearch is called periodically to search for providers of a
// randomly chosen CID in the sesssion.
func (s *Session) handlePeriodicSearch(ctx context.Context) {
	randomWant := s.sw.RandomLiveWant()
	if !randomWant.Defined() {
		return
	}

	// TODO: come up with a better strategy for determining when to search
	// for new providers for blocks.
	s.findMorePeers(ctx, randomWant)

	s.broadcastWantHaves([]cid.Cid{randomWant})

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

// findMorePeers attempts to find more peers for a session by searching for
// providers for the given Cid
func (s *Session) findMorePeers(ctx context.Context, c cid.Cid) {
	go func(k cid.Cid) {
		for p := range s.providerFinder.FindProvidersAsync(ctx, k) {
			// When a provider indicates that it has a cid, it's equivalent to
			// the providing peer sending a HAVE
			s.sws.Update(p, nil, []cid.Cid{c}, nil)
		}
	}(c)
}

// handleReceive is called when the session receives a message from a peer
func (s *Session) handleReceive(ks []cid.Cid, haves []cid.Cid) {
	// If the peer sent blocks or HAVEs then it will be added to
	// the session, so we can stop broadcasting wants (we start
	// sending wants to peers in the session directly)
	if len(ks) > 0 || len(haves) > 0 {
		s.peersDiscovered = true
	}

	// Record which blocks have been received and figure out the total latency
	// for fetching the blocks
	wanted, totalLatency := s.sw.BlocksReceived(ks)
	if len(wanted) == 0 {
		return
	}

	// Record latency
	s.latencyTrkr.receiveUpdate(len(wanted), totalLatency)

	s.idleTick.Stop()

	// We've received new wanted blocks, so reset the number of ticks
	// that have occurred since the last new block
	s.consecutiveTicks = 0

	s.resetIdleTick()
}

// wantBlocks is called when blocks are requested by the client
func (s *Session) wantBlocks(newks []cid.Cid) {
	// Tell the sessionWants tracker that that the wants have been requested
	s.sw.BlocksRequested(newks)

	// If we have discovered peers already, the sessionWantSender will
	// send wants to them
	if s.peersDiscovered {
		return
	}

	// No peers discovered yet, broadcast some want-haves
	ks := s.sw.GetNextWants()
	if len(ks) > 0 {
		log.Infow("No peers - broadcasting", "session", s.id, "want-count", len(ks))
		s.broadcastWantHaves(ks)
	}
}

// Send want-haves to all connected peers
func (s *Session) broadcastWantHaves(wants []cid.Cid) {
	log.Debugw("broadcastWantHaves", "session", s.id, "cids", wants)
	s.pm.BroadcastWantHaves(s.id, wants)
}

func (s *Session) logReceiveFrom(msg *notifications.IncomingMessage) {
	// Save some CPU cycles if log level is higher than debug
	if ce := sflog.Check(zap.DebugLevel, "Bitswap <- rcv message"); ce == nil {
		return
	}

	from := msg.From
	for _, b := range msg.Blks {
		log.Debugw("Bitswap <- block", "local", s.self, "from", from, "cid", b.Cid(), "session", s.id)
	}
	for _, c := range msg.Haves {
		log.Debugw("Bitswap <- HAVE", "local", s.self, "from", from, "cid", c, "session", s.id)
	}
	for _, c := range msg.DontHaves {
		log.Debugw("Bitswap <- DONT_HAVE", "local", s.self, "from", from, "cid", c, "session", s.id)
	}
}

// The session will broadcast if it has outstanding wants and doesn't receive
// any blocks for some time.
// The length of time is calculated
// - initially
//   as a fixed delay
// - once some blocks are received
//   from a base delay and average latency, with a backoff
func (s *Session) resetIdleTick() {
	var tickDelay time.Duration
	if !s.latencyTrkr.hasLatency() {
		tickDelay = s.initialSearchDelay
	} else {
		avLat := s.latencyTrkr.averageLatency()
		tickDelay = s.baseTickDelay + (3 * avLat)
	}
	tickDelay = tickDelay * time.Duration(1+s.consecutiveTicks)
	s.idleTick.Reset(tickDelay)
}

// latencyTracker keeps track of the average latency between sending a want
// and receiving the corresponding block
type latencyTracker struct {
	totalLatency time.Duration
	count        int
}

func (lt *latencyTracker) hasLatency() bool {
	return lt.totalLatency > 0 && lt.count > 0
}

func (lt *latencyTracker) averageLatency() time.Duration {
	return lt.totalLatency / time.Duration(lt.count)
}

func (lt *latencyTracker) receiveUpdate(count int, totalLatency time.Duration) {
	lt.totalLatency += totalLatency
	lt.count += count
}
