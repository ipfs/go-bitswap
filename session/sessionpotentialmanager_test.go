package session

import (
	"context"
	"sync"
	"testing"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	bspm "github.com/ipfs/go-bitswap/peermanager"
	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type sentWants struct {
	p          peer.ID
	wantHaves  *cid.Set
	wantBlocks *cid.Set
}

type mockThresholdManager struct {
	threshold float64
}

func (mtm *mockThresholdManager) PotentialThreshold() float64 {
	return mtm.threshold
}

func (mtm *mockThresholdManager) IdleTimeout() {
}

func (mtm *mockThresholdManager) Received(hits int, misses int) {
}

type mockPeerManager struct {
	peerSessions sync.Map
	peerSends    sync.Map
	sendTrigger  chan struct{}
}

func newMockPeerManager() *mockPeerManager {
	return &mockPeerManager{
		sendTrigger: make(chan struct{}, 1),
	}
}

func (pm *mockPeerManager) RegisterSession(p peer.ID, sess bspm.Session) bool {
	pm.peerSessions.Store(p, sess)
	return true
}

func (pm *mockPeerManager) UnregisterSession(sesid uint64) {
}

func (pm *mockPeerManager) RequestToken(peer.ID) bool {
	return true
}

func (pm *mockPeerManager) SendWants(ctx context.Context, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	swi, _ := pm.peerSends.LoadOrStore(p, sentWants{p, cid.NewSet(), cid.NewSet()})
	sw := swi.(sentWants)
	for _, c := range wantBlocks {
		sw.wantBlocks.Add(c)
	}
	for _, c := range wantHaves {
		sw.wantHaves.Add(c)
	}

	pm.sendTrigger <- struct{}{}
}

func (pm *mockPeerManager) waitNextWants() {
	<-pm.sendTrigger

	time.Sleep(5 * time.Millisecond)

	for {
		select {
		case <-pm.sendTrigger:
		default:
			return
		}
	}
}

func (pm *mockPeerManager) flushNextWants() {
	time.Sleep(5 * time.Millisecond)

	for {
		select {
		case <-pm.sendTrigger:
		default:
			return
		}
	}
}

func (pm *mockPeerManager) wantSent() bool {
	time.Sleep(5 * time.Millisecond)

	select {
	case <-pm.sendTrigger:
		return true
	default:
		return false
	}
}

func (pm *mockPeerManager) clearWants() {
	pm.peerSends.Range(func(k, v interface{}) bool {
		pm.peerSends.Delete(k)
		return true
	})
}

func TestSendWants(t *testing.T) {
	cids := testutil.GenerateCids(4)
	peers := testutil.GeneratePeers(3)
	peerA := peers[0]
	peerB := peers[1]
	peerC := peers[2]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	potentialThreshold := 1.0
	ptm := &mockThresholdManager{potentialThreshold}
	spm := newSessionPotentialManager(sid, pm, bpm, ptm, onSend, onPeersExhausted)

	go spm.Run(context.Background())

	if spm.ID() != sid {
		t.Fatal("Wrong session id")
	}

	// add cid0, cid1
	blkCids0 := cids[0:2]
	spm.Add(blkCids0)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	swi, ok := pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw := swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), blkCids0) {
		t.Fatal("Wrong keys")
	}
	if sw.wantHaves.Len() > 0 {
		t.Fatal("Expecting no want-haves")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerB: HAVE cid0
	spm.Update(peerB, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerB: want-block cid0, cid1
	// peerA: want-have cid0, cid1
	swi, ok = pm.peerSends.Load(peerB)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), blkCids0) {
		t.Fatal("Wrong keys")
	}
	if sw.wantHaves.Len() > 0 {
		t.Fatal("Expecting no want-haves")
	}

	swi, ok = pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if sw.wantBlocks.Len() > 0 {
		t.Fatal("Expecting no want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantHaves.Keys(), blkCids0) {
		t.Fatal("Wrong keys")
	}

	// Clear wants
	pm.clearWants()

	// add cid2, cid3
	blkCids1 := cids[2:4]
	spm.Add(blkCids1)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid2, cid3
	// peerB: want-block cid2, cid3
	swi, ok = pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), blkCids1) {
		t.Fatal("Wrong keys")
	}

	swi, ok = pm.peerSends.Load(peerB)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), blkCids1) {
		t.Fatal("Wrong keys")
	}

	// Clear wants
	pm.clearWants()

	// peerC: HAVE cid0
	spm.Update(peerC, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Each CID has been sent to peerA and peerB, so each want has a
	// sent potential of 0.5 + 0.5 = 1.0
	// But the potential threshold is 1.0, so no new want-blocks should be
	// sent to peerC, only want-haves
	swi, ok = pm.peerSends.Load(peerC)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if sw.wantBlocks.Len() > 0 {
		t.Fatal("Expecting no want-blocks")
	}
	if sw.wantHaves.Len() == 0 {
		t.Fatal("Expecting want-haves")
	}
}

func TestThresholdIncrease(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	potentialThreshold := 0.5
	ptm := &mockThresholdManager{potentialThreshold}
	spm := newSessionPotentialManager(sid, pm, bpm, ptm, onSend, onPeersExhausted)

	go spm.Run(context.Background())

	// add cid0, cid1
	spm.Add(cids)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	swi, ok := pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw := swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerB: HAVE cid0
	spm.Update(peerB, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should not have sent anything because wants were already sent to peer A
	// so wants now have a potential of 0.5, which is >= the threshold (0.5)
	swi, ok = pm.peerSends.Load(peerB)
	if ok && swi.(sentWants).wantBlocks.Len() > 0 {
		t.Fatal("Expected no wants sent to peer")
	}

	// Manually increase threshold and trigger update.
	// Note: the threshold is compared to the potential that has already
	// been sent. As long as the sent potential is below the threshold,
	// we can sent a want with any amount of potential.
	ptm.threshold += 0.1
	spm.Update(peerA, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should now have sent want-blocks to peerB
	swi, ok = pm.peerSends.Load(peerB)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}
}

func TestReceiveBlock(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	potentialThreshold := 1.0
	ptm := &mockThresholdManager{potentialThreshold}
	spm := newSessionPotentialManager(sid, pm, bpm, ptm, onSend, onPeersExhausted)

	go spm.Run(context.Background())

	// add cid0, cid1
	spm.Add(cids)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	swi, ok := pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw := swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerA: receive block cid0
	spm.Update(peerA, cids[0:1], []cid.Cid{}, []cid.Cid{}, false)
	// peerB: HAVE cid1
	spm.Update(peerB, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerB: want-block cid1
	// (should not have sent want-block for cid0 because block0 has already
	// been received)
	swi, ok = pm.peerSends.Load(peerB)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids[1:2]) {
		t.Fatal("Wrong keys")
	}
}

func TestReceiveDontHave(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	potentialThreshold := 0.5
	ptm := &mockThresholdManager{potentialThreshold}
	spm := newSessionPotentialManager(sid, pm, bpm, ptm, onSend, onPeersExhausted)

	go spm.Run(context.Background())

	// add cid0, cid1
	spm.Add(cids)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	swi, ok := pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw := swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerB: HAVE cid0
	spm.Update(peerB, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should not have sent anything because wants were already sent to peer A
	// so wants now have a potential of 0.5, which is >= the threshold (0.5)
	swi, ok = pm.peerSends.Load(peerB)
	if ok && swi.(sentWants).wantBlocks.Len() > 0 {
		t.Fatal("Expected no wants sent to peer")
	}

	// peerA: DONT_HAVE cid0
	bpm.ReceiveFrom(peerA, []cid.Cid{}, cids[0:1])
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{}, cids[0:1], true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should now have sent want-block cid0 to peerB
	swi, ok = pm.peerSends.Load(peerB)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids[0:1]) {
		t.Fatal("Wrong keys")
	}
}

func TestPeerUnavailable(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	potentialThreshold := 0.5
	ptm := &mockThresholdManager{potentialThreshold}
	spm := newSessionPotentialManager(sid, pm, bpm, ptm, onSend, onPeersExhausted)

	go spm.Run(context.Background())

	// add cid0, cid1
	spm.Add(cids)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	swi, ok := pm.peerSends.Load(peerA)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw := swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerB: HAVE cid0
	spm.Update(peerB, []cid.Cid{}, cids[0:1], []cid.Cid{}, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should not have sent anything because wants were already sent to peer A
	// so wants now have a potential of 0.5, which is >= the threshold (0.5)
	swi, ok = pm.peerSends.Load(peerB)
	if ok && swi.(sentWants).wantBlocks.Len() > 0 {
		t.Fatal("Expected no wants sent to peer")
	}

	// peerA becomes unavailable
	spm.SignalAvailability(peerA, false)

	// Wait for processing to complete
	pm.waitNextWants()

	// Should now have sent want-block cid0, cid1 to peerB
	swi, ok = pm.peerSends.Load(peerB)
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	sw = swi.(sentWants)
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}
}

func TestPeersExhausted(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	potentialThreshold := 0.5
	ptm := &mockThresholdManager{potentialThreshold}

	var exhausted []cid.Cid
	onPeersExhausted := func(ks []cid.Cid) {
		exhausted = append(exhausted, ks...)
	}
	spm := newSessionPotentialManager(sid, pm, bpm, ptm, onSend, onPeersExhausted)

	go spm.Run(context.Background())

	// add cid0, cid1
	spm.Add(cids)

	// peerA: DONT_HAVE cid0
	bpm.ReceiveFrom(peerA, []cid.Cid{}, cids[0:1])
	// Note: this also registers peer A as being available
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{}, cids[0:1], true)

	time.Sleep(5 * time.Millisecond)

	// All available peers (peer A) have sent us a DONT_HAVE for cid0,
	// so expect that onPeersExhausted() will be called with cid0
	if !testutil.MatchKeysIgnoreOrder(exhausted, cids[0:1]) {
		t.Fatal("Wrong keys")
	}
	// Clear exhausted cids
	exhausted = []cid.Cid{}

	// peerB: DONT_HAVE cid0, cid1
	bpm.ReceiveFrom(peerB, []cid.Cid{}, cids)
	spm.Update(peerB, []cid.Cid{}, []cid.Cid{}, cids, true)

	// Wait for processing to complete
	pm.flushNextWants()

	// All available peers (peer A and peer B) have sent us a DONT_HAVE
	// for cid0, but we already called onPeersExhausted with cid0, so it
	// should not be called again
	if len(exhausted) > 0 {
		t.Fatal("Wrong keys")
	}

	// peerA: DONT_HAVE cid1
	bpm.ReceiveFrom(peerA, []cid.Cid{}, cids[1:])
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{}, cids[1:], false)

	// Wait for processing to complete
	pm.flushNextWants()

	// All available peers (peer A and peer B) have sent us a DONT_HAVE for
	// cid1, so expect that onPeersExhausted() will be called with cid1
	if !testutil.MatchKeysIgnoreOrder(exhausted, cids[1:]) {
		t.Fatal("Wrong keys")
	}
}
