package session

import (
	"context"
	"sync"
	"testing"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	bsspm "github.com/ipfs/go-bitswap/internal/sessionpeermanager"
	"github.com/ipfs/go-bitswap/internal/testutil"
	bswrm "github.com/ipfs/go-bitswap/internal/wantrequestmanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type mockSessionMgr struct {
	lk      sync.Mutex
	removes []uint64
}

func newMockSessionMgr() *mockSessionMgr {
	return &mockSessionMgr{}
}

func (msm *mockSessionMgr) removedSessions() []uint64 {
	msm.lk.Lock()
	defer msm.lk.Unlock()
	return msm.removes
}

func (msm *mockSessionMgr) RemoveSession(sesid uint64) {
	msm.lk.Lock()
	defer msm.lk.Unlock()
	msm.removes = append(msm.removes, sesid)
}

func newFakeSessionPeerManager() *bsspm.SessionPeerManager {
	return bsspm.New(1, newFakePeerTagger())
}

func newFakePeerTagger() *fakePeerTagger {
	return &fakePeerTagger{
		protectedPeers: make(map[peer.ID]map[string]struct{}),
	}
}

type fakePeerTagger struct {
	lk             sync.Mutex
	protectedPeers map[peer.ID]map[string]struct{}
}

func (fpt *fakePeerTagger) TagPeer(p peer.ID, tag string, val int) {}
func (fpt *fakePeerTagger) UntagPeer(p peer.ID, tag string)        {}

func (fpt *fakePeerTagger) Protect(p peer.ID, tag string) {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	tags, ok := fpt.protectedPeers[p]
	if !ok {
		tags = make(map[string]struct{})
		fpt.protectedPeers[p] = tags
	}
	tags[tag] = struct{}{}
}

func (fpt *fakePeerTagger) Unprotect(p peer.ID, tag string) bool {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	if tags, ok := fpt.protectedPeers[p]; ok {
		delete(tags, tag)
		return len(tags) > 0
	}

	return false
}

func (fpt *fakePeerTagger) isProtected(p peer.ID) bool {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	return len(fpt.protectedPeers[p]) > 0
}

type fakeProviderFinder struct {
	findMorePeersRequested chan cid.Cid
}

func newFakeProviderFinder() *fakeProviderFinder {
	return &fakeProviderFinder{
		findMorePeersRequested: make(chan cid.Cid, 1),
	}
}

func (fpf *fakeProviderFinder) FindProvidersAsync(ctx context.Context, k cid.Cid) <-chan peer.ID {
	go func() {
		select {
		case fpf.findMorePeersRequested <- k:
		case <-ctx.Done():
		}
	}()

	return make(chan peer.ID)
}

type bcstWantReq struct {
	cids []cid.Cid
}

type fakePeerManager struct {
	bcstWantReqs chan bcstWantReq
	lk           sync.Mutex
	cncls        []cid.Cid
}

func newFakePeerManager() *fakePeerManager {
	return &fakePeerManager{
		bcstWantReqs: make(chan bcstWantReq, 1),
	}
}

func (pm *fakePeerManager) RegisterSession(peer.ID, bspm.Session) {}
func (pm *fakePeerManager) UnregisterSession(uint64)              {}
func (pm *fakePeerManager) SendWants(sid uint64, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
}

func (pm *fakePeerManager) SendCancels(sid uint64, cancels []cid.Cid) {
	pm.lk.Lock()
	defer pm.lk.Unlock()
	pm.cncls = append(pm.cncls, cancels...)
}
func (pm *fakePeerManager) cancels() []cid.Cid {
	pm.lk.Lock()
	defer pm.lk.Unlock()
	return pm.cncls
}

func (pm *fakePeerManager) clear() {
	pm.lk.Lock()
	defer pm.lk.Unlock()
	pm.cncls = nil
}

func (pm *fakePeerManager) BroadcastWantHaves(sid uint64, cids []cid.Cid) {
	pm.bcstWantReqs <- bcstWantReq{cids}
}

func createBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
}

func TestSessionGetBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, fpm, bpm, wrm, time.Second, delay.Fixed(time.Minute), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}

	_, err := session.GetBlocks(ctx, cids)

	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Wait for initial want request
	receivedWantReq := <-fpm.bcstWantReqs

	// Should have sent out broadcast request for wants
	if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}

	// Simulate receiving HAVEs from several peers
	peers := testutil.GeneratePeers(5)
	for i, p := range peers {
		blk := blks[testutil.IndexOf(blks, receivedWantReq.cids[i])]
		_, err := wrm.ReceiveMessage(&bswrm.IncomingMessage{
			From:  p,
			Haves: []cid.Cid{blk.Cid()},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(10 * time.Millisecond)

	// Verify new peers were recorded
	if !testutil.MatchPeersIgnoreOrder(fspm.Peers(), peers) {
		t.Fatal("peers not recorded by the peer manager")
	}

	// Simulate receiving DONT_HAVE for a CID
	_, err = wrm.ReceiveMessage(&bswrm.IncomingMessage{
		From:      peers[0],
		DontHaves: []cid.Cid{blks[0].Cid()},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)

	// Simulate receiving block for a CID
	_, err = wrm.ReceiveMessage(&bswrm.IncomingMessage{
		From: peers[1],
		Blks: []blocks.Block{blks[0]},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)

	// Should send a cancel for the block
	cancels := fpm.cancels()
	if !testutil.MatchKeysIgnoreOrder(cancels, []cid.Cid{blks[0].Cid()}) {
		t.Fatal("expected session to send cancel for received block")
	}
	fpm.clear()

	// Shut down session
	cancel()

	time.Sleep(10 * time.Millisecond)

	// Should send a cancel for all remaining blocks
	cancels = fpm.cancels()
	if !testutil.MatchKeysIgnoreOrder(cancels, cids[1:]) {
		t.Fatal("expected session to send cancel for received block")
	}

	// Verify session was removed
	removed := sm.removedSessions()
	if len(removed) != 1 || removed[0] != id {
		t.Fatal("expected session to be removed")
	}
}

func TestSessionFindMorePeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, fpm, bpm, wrm, time.Second, delay.Fixed(time.Minute), "")
	session.SetBaseTickDelay(200 * time.Microsecond)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}

	// Request blocks
	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// The session should initially broadcast want-haves
	select {
	case <-fpm.bcstWantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make first want request ")
	}

	// receive a block to trigger a tick reset
	time.Sleep(20 * time.Millisecond) // need to make sure some latency registers
	// or there will be no tick set -- time precision on Windows in go is in the
	// millisecond range
	p := testutil.GeneratePeers(1)[0]

	// Simulate receiving block for a CID
	_, err = wrm.ReceiveMessage(&bswrm.IncomingMessage{
		From: p,
		Blks: []blocks.Block{blks[0]},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The session should now time out waiting for a response and broadcast
	// want-haves again
	select {
	case <-fpm.bcstWantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make second want request ")
	}

	// The session should keep broadcasting periodically until it receives a response
	select {
	case receivedWantReq := <-fpm.bcstWantReqs:
		if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
			t.Fatal("did not rebroadcast whole live list")
		}
		// Make sure the first block is not included because it has already
		// been received
		for _, c := range receivedWantReq.cids {
			if c.Equals(cids[0]) {
				t.Fatal("should not braodcast block that was already received")
			}
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// The session should eventually try to find more peers
	select {
	case <-fpf.findMorePeersRequested:
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}
}

func TestSessionOnPeersExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, fpm, bpm, wrm, time.Second, delay.Fixed(time.Minute), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit + 5)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	_, err := session.GetBlocks(ctx, cids)

	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Wait for initial want request
	receivedWantReq := <-fpm.bcstWantReqs

	// Should have sent out broadcast request for wants
	if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}

	// Signal that all peers have send DONT_HAVE for two of the wants
	session.onPeersExhausted(cids[len(cids)-2:])

	// Wait for want request
	receivedWantReq = <-fpm.bcstWantReqs

	// Should have sent out broadcast request for wants
	if len(receivedWantReq.cids) != 2 {
		t.Fatal("did not enqueue correct initial number of wants")
	}
}

func TestSessionFailingToGetFirstBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, fpm, bpm, wrm, 10*time.Millisecond, delay.Fixed(100*time.Millisecond), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(4)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	startTick := time.Now()

	// Get blocks
	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// The session should initially broadcast want-haves
	select {
	case <-fpm.bcstWantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make first want request ")
	}

	// Verify a broadcast was made
	select {
	case receivedWantReq := <-fpm.bcstWantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Wait for a request to find more peers to occur
	select {
	case k := <-fpf.findMorePeersRequested:
		if testutil.IndexOf(blks, k) == -1 {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}
	firstTickLength := time.Since(startTick)

	// Wait for another broadcast to occur
	select {
	case receivedWantReq := <-fpm.bcstWantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Wait for another broadcast to occur
	startTick = time.Now()
	select {
	case receivedWantReq := <-fpm.bcstWantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Tick should take longer
	consecutiveTickLength := time.Since(startTick)
	if firstTickLength > consecutiveTickLength {
		t.Fatal("Should have increased tick length after first consecutive tick")
	}

	// Wait for another broadcast to occur
	startTick = time.Now()
	select {
	case receivedWantReq := <-fpm.bcstWantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Tick should take longer
	secondConsecutiveTickLength := time.Since(startTick)
	if consecutiveTickLength > secondConsecutiveTickLength {
		t.Fatal("Should have increased tick length after first consecutive tick")
	}

	// Should not have tried to find peers on consecutive ticks
	select {
	case <-fpf.findMorePeersRequested:
		t.Fatal("Should not have tried to find peers on consecutive ticks")
	default:
	}

	// Wait for rebroadcast to occur
	select {
	case k := <-fpf.findMorePeersRequested:
		if testutil.IndexOf(blks, k) == -1 {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not rebroadcast to find more peers")
	}
}

func TestSessionCtxCancelClosesGetBlocksChannel(t *testing.T) {
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session := New(sessctx, sm, id, fspm, fpf, fpm, bpm, wrm, time.Second, delay.Fixed(time.Minute), "")

	timerCtx, timerCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer timerCancel()

	// Request a block with a new context
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(1)
	getctx, getcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer getcancel()

	getBlocksCh, err := session.GetBlocks(getctx, []cid.Cid{blks[0].Cid()})
	if err != nil {
		t.Fatal("error getting blocks")
	}

	time.Sleep(10 * time.Millisecond)

	// Cancel the session context
	sesscancel()

	// Expect the GetBlocks() channel to be closed
	select {
	case _, ok := <-getBlocksCh:
		if ok {
			t.Fatal("expected channel to be closed but was not closed")
		}
	case <-timerCtx.Done():
		t.Fatal("expected channel to be closed before timeout")
	}

	time.Sleep(10 * time.Millisecond)

	// Expect cancels to have been sent for the wanted keys
	cancels := fpm.cancels()
	if !testutil.MatchKeysIgnoreOrder(cancels, []cid.Cid{blks[0].Cid()}) {
		t.Fatal("expected session to send cancel for wanted block")
	}

	// Verify session was removed
	removed := sm.removedSessions()
	if len(removed) != 1 || removed[0] != id {
		t.Fatal("expected session to be removed")
	}
}

func TestSessionOnShutdownCalled(t *testing.T) {
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer sesscancel()
	session := New(sessctx, sm, id, fspm, fpf, fpm, bpm, wrm, time.Second, delay.Fixed(time.Minute), "")

	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(1)
	_, err := session.GetBlocks(context.Background(), []cid.Cid{blks[0].Cid()})
	if err != nil {
		t.Fatal("error getting blocks")
	}

	time.Sleep(10 * time.Millisecond)

	// Shutdown the session
	session.Shutdown()

	time.Sleep(10 * time.Millisecond)

	// Expect cancels to have been sent for the wanted keys
	cancels := fpm.cancels()
	if !testutil.MatchKeysIgnoreOrder(cancels, []cid.Cid{blks[0].Cid()}) {
		t.Fatal("expected session to send cancel for wanted block")
	}

	// Verify session was removed
	removed := sm.removedSessions()
	if len(removed) != 1 || removed[0] != id {
		t.Fatal("expected session to be removed")
	}
}

func TestSessionReceiveMessageAfterCtxCancel(t *testing.T) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 20*time.Millisecond)
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	bpm := bsbpm.New()
	wrm := bswrm.New(createBlockstore(), bpm)
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, fpm, bpm, wrm, time.Second, delay.Fixed(time.Minute), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(2)
	cids := []cid.Cid{blks[0].Cid(), blks[1].Cid()}

	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Wait for initial want request
	<-fpm.bcstWantReqs

	// Shut down session
	cancelCtx()

	// Simulate receiving block for a CID
	p := testutil.GeneratePeers(1)[0]
	_, err = wrm.ReceiveMessage(&bswrm.IncomingMessage{
		From: p,
		Blks: []blocks.Block{blks[0]},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Millisecond)

	// If we don't get a panic then the test is considered passing
}
