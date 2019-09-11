package peermanager

import (
	"context"
	"sync"

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

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID) PeerQueue

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
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerManager {
	return &PeerManager{
		peerQueues:      make(map[peer.ID]*peerQueueInstance),
		pwm:             newPeerWantManager(),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
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
func (pm *PeerManager) Connected(p peer.ID, initialWantHaves []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	pq := pm.getOrCreate(p)

	if pq.refcnt == 0 {
		// Broadcast any live want-haves to the newly connected peers
		pq.pq.AddBroadcastWantHaves(initialWantHaves)
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

	delete(pm.peerQueues, p)
	pq.pq.Shutdown()
	pm.pwm.RemovePeer(p)
}

func (pm *PeerManager) BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	for p, ks := range pm.pwm.BroadcastWantHaves(wantHaves) {
		pqi := pm.getOrCreate(p)
		pqi.pq.AddBroadcastWantHaves(ks)
	}
}

func (pm *PeerManager) SendWants(ctx context.Context, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	pqi := pm.getOrCreate(p)
	wblks, whvs := pm.pwm.SendWants(p, wantBlocks, wantHaves)
	pqi.pq.AddWants(wblks, whvs)
}

func (pm *PeerManager) SendCancels(ctx context.Context, cancelKs []cid.Cid) {
	pm.Lock()
	defer pm.Unlock()

	// Send a CANCEL to each peer that has been sent a want-block or want-have
	for p, ks := range pm.pwm.SendCancels(cancelKs) {
		pqi := pm.getOrCreate(p)
		pqi.pq.AddCancels(ks)
	}
}

func (pm *PeerManager) PeerCanSendWants(p peer.ID, wants []cid.Cid) []cid.Cid {
	pm.RLock()
	defer pm.RUnlock()

	return pm.pwm.PeerCanSendWants(p, wants)
}

func (pm *PeerManager) PeersCanSendWantBlock(c cid.Cid, peers []peer.ID) []peer.ID {
	pm.RLock()
	defer pm.RUnlock()

	return pm.pwm.PeersCanSendWantBlock(c, peers)
}

func (pm *PeerManager) CurrentWants() []cid.Cid {
	pm.RLock()
	defer pm.RUnlock()

	return pm.pwm.GetWantBlocks()
}

func (pm *PeerManager) getOrCreate(p peer.ID) *peerQueueInstance {
	pqi, ok := pm.peerQueues[p]
	if !ok {
		pq := pm.createPeerQueue(pm.ctx, p)
		pq.Startup()
		pqi = &peerQueueInstance{0, pq}
		pm.peerQueues[p] = pqi
	}
	return pqi
}
