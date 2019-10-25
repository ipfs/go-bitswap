package peermanager

import (
	"context"
	"sync"

	lu "github.com/ipfs/go-bitswap/logutil"
	"github.com/ipfs/go-metrics-interface"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bs:pmgr")

// PeerQueue provides a queue of messages to be sent for a single peer.
type PeerQueue interface {
	AddBroadcastWantHaves([]cid.Cid)
	AddWants([]cid.Cid, []cid.Cid)
	AddCancels([]cid.Cid)
	Startup()
	Shutdown()
}

type Session interface {
	ID() uint64
	SignalAvailability(peer.ID, bool)
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID, supportsHave bool) PeerQueue

type peerQueueInstance struct {
	refcnt int
	pq     PeerQueue
}

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	sync.RWMutex

	// peerQueues -- interact through internal utility functions get/set/remove/iterate
	peerQueues map[peer.ID]*peerQueueInstance
	pwm        *peerWantManager

	createPeerQueue PeerQueueFactory
	ctx             context.Context

	psLk         sync.RWMutex
	sessions     map[uint64]Session
	peerSessions map[peer.ID]map[uint64]struct{}

	self peer.ID
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory, self peer.ID) *PeerManager {
	wantGauge := metrics.NewCtx(ctx, "wantlist_total", "Number of items in wantlist.").Gauge()
	return &PeerManager{
		peerQueues:      make(map[peer.ID]*peerQueueInstance),
		pwm:             newPeerWantManager(wantGauge),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
		self:            self,

		sessions:     make(map[uint64]Session),
		peerSessions: make(map[peer.ID]map[uint64]struct{}),
	}
}

func (pm *PeerManager) AvailablePeers() []peer.ID {
	// TODO: Rate-limit peers
	return pm.ConnectedPeers()
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager) ConnectedPeers() []peer.ID {
	pm.RLock()
	defer pm.RUnlock()

	peers := make([]peer.ID, 0, len(pm.peerQueues))
	for p := range pm.peerQueues {
		peers = append(peers, p)
	}
	return peers
}

// Connected is called to add a new peer to the pool, and send it an initial set
// of wants.
func (pm *PeerManager) Connected(p peer.ID, supportsHave bool, initialWantHaves []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	pq := pm.getOrCreate(p, supportsHave)

	if pq.refcnt == 0 {
		// Broadcast any live want-haves to the newly connected peers
		pq.pq.AddBroadcastWantHaves(initialWantHaves)
		// Inform the sessions that the peer has connected
		pm.signalAvailability(p, true)
	}

	pq.refcnt++
	pm.pwm.AddPeer(p)
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	pm.Lock()
	defer pm.Unlock()

	pq, ok := pm.peerQueues[p]

	if !ok {
		return
	}

	pq.refcnt--
	if pq.refcnt > 0 {
		return
	}

	// Inform the sessions that the peer has disconnected
	pm.signalAvailability(p, false)

	// Clean up the peer
	delete(pm.peerQueues, p)
	pq.pq.Shutdown()
	pm.pwm.RemovePeer(p)
}

func (pm *PeerManager) BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	for p, ks := range pm.pwm.BroadcastWantHaves(wantHaves) {
		if pqi, ok := pm.peerQueues[p]; ok {
			pqi.pq.AddBroadcastWantHaves(ks)
		}
	}
}

func (pm *PeerManager) SendWants(ctx context.Context, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	if pqi, ok := pm.peerQueues[p]; ok {
		wblks, whvs := pm.pwm.SendWants(p, wantBlocks, wantHaves)
		pqi.pq.AddWants(wblks, whvs)
	}
}

func (pm *PeerManager) SendCancels(ctx context.Context, cancelKs []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	// Send a CANCEL to each peer that has been sent a want-block or want-have
	for p, ks := range pm.pwm.SendCancels(cancelKs) {
		if pqi, ok := pm.peerQueues[p]; ok {
			pqi.pq.AddCancels(ks)
		}
	}
}

func (pm *PeerManager) CurrentWants() []cid.Cid {
	pm.RLock()
	defer pm.RUnlock()

	return pm.pwm.GetWantBlocks()
}

func (pm *PeerManager) CurrentWantHaves() []cid.Cid {
	pm.RLock()
	defer pm.RUnlock()

	return pm.pwm.GetWantHaves()
}

func (pm *PeerManager) getOrCreate(p peer.ID, supportsHave bool) *peerQueueInstance {
	pqi, ok := pm.peerQueues[p]
	if !ok {
		pq := pm.createPeerQueue(pm.ctx, p, supportsHave)
		pq.Startup()
		pqi = &peerQueueInstance{0, pq}
		pm.peerQueues[p] = pqi
	}
	return pqi
}

func (pm *PeerManager) Trace() {
	var peers []string
	for p := range pm.peerQueues {
		peers = append(peers, lu.P(p))
	}
	log.Warningf("%s peer want manager\npeers %s\n%s\n", pm.self, peers, pm.pwm)
}

func (pm *PeerManager) RegisterSession(p peer.ID, s Session) bool {
	pm.psLk.Lock()
	defer pm.psLk.Unlock()

	if _, ok := pm.sessions[s.ID()]; !ok {
		pm.sessions[s.ID()] = s
	}

	if _, ok := pm.peerSessions[p]; !ok {
		pm.peerSessions[p] = make(map[uint64]struct{})
	}
	pm.peerSessions[p][s.ID()] = struct{}{}

	_, ok := pm.peerQueues[p]
	return ok
}

func (pm *PeerManager) UnregisterSession(ses uint64) {
	pm.psLk.Lock()
	defer pm.psLk.Unlock()

	for p := range pm.peerSessions {
		delete(pm.peerSessions[p], ses)
		if len(pm.peerSessions[p]) == 0 {
			delete(pm.peerSessions, p)
		}
	}

	delete(pm.sessions, ses)
}

func (pm *PeerManager) RequestToken(p peer.ID) bool {
	// TODO: rate limiting
	return true
}

func (pm *PeerManager) signalAvailability(p peer.ID, isConnected bool) {
	for p, sesIds := range pm.peerSessions {
		for sesId := range sesIds {
			if s, ok := pm.sessions[sesId]; ok {
				s.SignalAvailability(p, isConnected)
			}
		}
	}
}
