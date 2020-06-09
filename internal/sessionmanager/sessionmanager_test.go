package sessionmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	peer "github.com/libp2p/go-libp2p-core/peer"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	bssession "github.com/ipfs/go-bitswap/internal/session"
	bswrm "github.com/ipfs/go-bitswap/internal/wantrequestmanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

type fakeSession struct {
	ks             []cid.Cid
	wantBlocks     []cid.Cid
	wantHaves      []cid.Cid
	id             uint64
	lk             sync.Mutex
	shutdownCalled bool
}

func (*fakeSession) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, nil
}
func (*fakeSession) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	return nil, nil
}
func (fs *fakeSession) ID() uint64 {
	return fs.id
}
func (fs *fakeSession) Shutdown() {
	fs.lk.Lock()
	defer fs.lk.Unlock()

	fs.shutdownCalled = true
}
func (fs *fakeSession) wasShutdownCalled() bool {
	fs.lk.Lock()
	defer fs.lk.Unlock()

	return fs.shutdownCalled
}

type fakeSesPeerManager struct {
}

func (*fakeSesPeerManager) Peers() []peer.ID        { return nil }
func (*fakeSesPeerManager) PeersDiscovered() bool   { return false }
func (*fakeSesPeerManager) Shutdown()               {}
func (*fakeSesPeerManager) AddPeer(peer.ID) bool    { return false }
func (*fakeSesPeerManager) RemovePeer(peer.ID) bool { return false }
func (*fakeSesPeerManager) HasPeers() bool          { return false }

type fakePeerManager struct {
}

func (*fakePeerManager) RegisterSession(peer.ID, bspm.Session) bool      { return true }
func (*fakePeerManager) UnregisterSession(uint64)                        {}
func (*fakePeerManager) SendWants(uint64, peer.ID, []cid.Cid, []cid.Cid) {}
func (*fakePeerManager) BroadcastWantHaves(uint64, []cid.Cid)            {}
func (fpm *fakePeerManager) SendCancels(sid uint64, cancels []cid.Cid)   {}
func (fpm *fakePeerManager) cancelled() []cid.Cid                        { return nil }

func sessionFactory(
	ctx context.Context,
	sm bssession.SessionManager,
	id uint64,
	sprm bssession.SessionPeerManager,
	pm bssession.PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	wrm *bswrm.WantRequestManager,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D,
	self peer.ID) Session {

	return &fakeSession{
		id: id,
	}
}

func peerManagerFactory(ctx context.Context, id uint64) bssession.SessionPeerManager {
	return &fakeSesPeerManager{}
}

func TestSessionManager(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wrm := bswrm.New(blockstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore())))
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(sessionFactory, peerManagerFactory, bpm, pm, wrm, "")

	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sm.Shutdown()

	if !firstSession.wasShutdownCalled() {
		t.Fatal("expected shutdown to be called")
	}
}
