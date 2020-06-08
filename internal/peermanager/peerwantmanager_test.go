package peermanager

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type gauge struct {
	count int
}

func (g *gauge) Inc() {
	g.count++
}
func (g *gauge) Dec() {
	g.count--
}

type mockPQ struct {
	bcst    []cid.Cid
	wbs     []cid.Cid
	whs     []cid.Cid
	cancels []cid.Cid
}

func (mpq *mockPQ) clear() {
	mpq.bcst = nil
	mpq.wbs = nil
	mpq.whs = nil
	mpq.cancels = nil
}

func (mpq *mockPQ) Startup()  {}
func (mpq *mockPQ) Shutdown() {}

func (mpq *mockPQ) AddBroadcastWantHaves(whs []cid.Cid) {
	mpq.bcst = append(mpq.bcst, whs...)
}
func (mpq *mockPQ) AddWants(wbs []cid.Cid, whs []cid.Cid) {
	mpq.wbs = append(mpq.wbs, wbs...)
	mpq.whs = append(mpq.whs, whs...)
}
func (mpq *mockPQ) AddCancels(cs []cid.Cid) {
	mpq.cancels = append(mpq.cancels, cs...)
}
func (mpq *mockPQ) ResponseReceived(ks []cid.Cid) {
}

func clearSent(pqs map[peer.ID]PeerQueue) {
	for _, pqi := range pqs {
		pqi.(*mockPQ).clear()
	}
}

func TestPWMEmpty(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	if len(pwm.getWantBlocks()) > 0 {
		t.Fatal("Expected GetWantBlocks() to have length 0")
	}
	if len(pwm.getWantHaves()) > 0 {
		t.Fatal("Expected GetWantHaves() to have length 0")
	}
}

func TestPWMBroadcastWantHaves(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid := uint64(1)
	peers := testutil.GeneratePeers(3)
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)
	cids3 := testutil.GenerateCids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		pwm.addPeer(pq, p)
		if len(pq.bcst) > 0 {
			t.Fatal("expected no broadcast wants")
		}
	}

	// Broadcast 2 cids to 2 peers
	pwm.broadcastWantHaves(sid, cids)
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 2 {
			t.Fatal("Expected 2 want-haves")
		}
		if !testutil.MatchKeysIgnoreOrder(pq.bcst, cids) {
			t.Fatal("Expected all cids to be broadcast")
		}
	}

	// Broadcasting same cids should have no effect
	clearSent(peerQueues)
	pwm.broadcastWantHaves(sid, cids)
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 0 {
			t.Fatal("Expected 0 want-haves")
		}
	}

	// Broadcast 2 other cids
	clearSent(peerQueues)
	pwm.broadcastWantHaves(sid, cids2)
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 2 {
			t.Fatal("Expected 2 want-haves")
		}
		if !testutil.MatchKeysIgnoreOrder(pq.bcst, cids2) {
			t.Fatal("Expected all new cids to be broadcast")
		}
	}

	// Broadcast mix of old and new cids
	clearSent(peerQueues)
	pwm.broadcastWantHaves(sid, append(cids, cids3...))
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 2 {
			t.Fatal("Expected 2 want-haves")
		}
		// Only new cids should be broadcast
		if !testutil.MatchKeysIgnoreOrder(pq.bcst, cids3) {
			t.Fatal("Expected all new cids to be broadcast")
		}
	}

	// Sending want-block for a cid should prevent broadcast to that peer
	clearSent(peerQueues)
	cids4 := testutil.GenerateCids(4)
	wantBlocks := []cid.Cid{cids4[0], cids4[2]}
	p0 := peers[0]
	p1 := peers[1]
	pwm.sendWants(sid, p0, wantBlocks, []cid.Cid{})

	pwm.broadcastWantHaves(sid, cids4)
	pq0 := peerQueues[p0].(*mockPQ)
	if len(pq0.bcst) != 2 { // only broadcast 2 / 4 want-haves
		t.Fatal("Expected 2 want-haves")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.bcst, []cid.Cid{cids4[1], cids4[3]}) {
		t.Fatalf("Expected unsent cids to be broadcast")
	}
	pq1 := peerQueues[p1].(*mockPQ)
	if len(pq1.bcst) != 4 { // broadcast all 4 want-haves
		t.Fatal("Expected 4 want-haves")
	}
	if !testutil.MatchKeysIgnoreOrder(pq1.bcst, cids4) {
		t.Fatal("Expected all cids to be broadcast")
	}

	allCids := cids
	allCids = append(allCids, cids2...)
	allCids = append(allCids, cids3...)
	allCids = append(allCids, cids4...)

	// Add another peer
	peer2 := peers[2]
	pq2 := &mockPQ{}
	peerQueues[peer2] = pq2
	pwm.addPeer(pq2, peer2)
	if !testutil.MatchKeysIgnoreOrder(pq2.bcst, allCids) {
		t.Fatalf("Expected all cids to be broadcast.")
	}

	clearSent(peerQueues)
	pwm.broadcastWantHaves(sid, allCids)
	if len(pq2.bcst) != 0 {
		t.Fatal("did not expect to have CIDs to broadcast")
	}
}

func TestPWMMultiSessionBroadcastWantHaves(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(2)

	p0 := peers[0]
	pq0 := &mockPQ{}
	p1 := peers[1]
	pq1 := &mockPQ{}

	// Add first peer
	pwm.addPeer(pq0, p0)
	if len(pq0.bcst) > 0 {
		t.Fatal("expected no broadcast wants")
	}

	// Broadcast 2 cids (to first peer)
	cids1 := testutil.GenerateCids(2)
	pwm.broadcastWantHaves(sid1, cids1)
	if len(pq0.bcst) != 2 {
		t.Fatal("wrong bcst count")
	}
	pq0.clear()

	// Broadcast (to first peer)
	// - 1 existing cid from same session
	// - 1 new cid from same session
	// - 1 existing cid from different session
	// - 1 new cid from different session
	cids2 := testutil.GenerateCids(2)
	pwm.broadcastWantHaves(sid1, append(cids1[:1], cids2[0]))
	pwm.broadcastWantHaves(sid2, append(cids1[1:2], cids2[1]))

	// Expect new cids to be broadcast
	if len(pq0.bcst) != 2 {
		t.Fatal("wrong bcst count")
	}
	pq0.clear()

	// Add second peer
	pwm.addPeer(pq1, p1)

	// Expect unique broadcast wants to be broadcast (to second peer)
	if len(pq1.bcst) != 4 {
		t.Fatal("expect unique cids to be broadcast")
	}
}

func TestPWMSendWants(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid := uint64(1)
	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		pwm.addPeer(pq, p)
	}
	pq0 := peerQueues[p0].(*mockPQ)
	pq1 := peerQueues[p1].(*mockPQ)

	// Send 2 want-blocks and 2 want-haves to p0
	clearSent(peerQueues)
	pwm.sendWants(sid, p0, cids, cids2)
	if !testutil.MatchKeysIgnoreOrder(pq0.wbs, cids) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.whs, cids2) {
		t.Fatal("Expected 2 want-haves")
	}

	// Send to p0
	// - 1 old want-block and 2 new want-blocks
	// - 1 old want-have  and 2 new want-haves
	clearSent(peerQueues)
	cids3 := testutil.GenerateCids(2)
	cids4 := testutil.GenerateCids(2)
	pwm.sendWants(sid, p0, append(cids3, cids[0]), append(cids4, cids2[0]))
	if !testutil.MatchKeysIgnoreOrder(pq0.wbs, cids3) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.whs, cids4) {
		t.Fatal("Expected 2 want-haves")
	}

	// Send to p0 as want-blocks: 1 new want-block, 1 old want-have
	clearSent(peerQueues)
	cids5 := testutil.GenerateCids(1)
	newWantBlockOldWantHave := append(cids5, cids2[0])
	pwm.sendWants(sid, p0, newWantBlockOldWantHave, []cid.Cid{})
	// If a want was sent as a want-have, it should be ok to now send it as a
	// want-block
	if !testutil.MatchKeysIgnoreOrder(pq0.wbs, newWantBlockOldWantHave) {
		t.Fatal("Expected 2 want-blocks")
	}
	if len(pq0.whs) != 0 {
		t.Fatal("Expected 0 want-haves")
	}

	// Send to p0 as want-haves: 1 new want-have, 1 old want-block
	clearSent(peerQueues)
	cids6 := testutil.GenerateCids(1)
	newWantHaveOldWantBlock := append(cids6, cids[0])
	pwm.sendWants(sid, p0, []cid.Cid{}, newWantHaveOldWantBlock)
	// If a want was previously sent as a want-block, it should not be
	// possible to now send it as a want-have
	if !testutil.MatchKeysIgnoreOrder(pq0.whs, cids6) {
		t.Fatal("Expected 1 want-have")
	}
	if len(pq0.wbs) != 0 {
		t.Fatal("Expected 0 want-blocks")
	}

	// Send 2 want-blocks and 2 want-haves to p1
	pwm.sendWants(sid, p1, cids, cids2)
	if !testutil.MatchKeysIgnoreOrder(pq1.wbs, cids) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pq1.whs, cids2) {
		t.Fatal("Expected 2 want-haves")
	}
}

func TestPWMMultiSessionSendWants(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	// Send
	// - want-have cid 1 for session 1
	// - want-have cid 2 for session 1
	// - want-have cid 2 for session 2
	cids1 := testutil.GenerateCids(2)
	pwm.sendWants(sid1, p0, nil, cids1)
	pwm.sendWants(sid2, p0, nil, cids1[1:2])
	if len(pq0.whs) != 2 {
		t.Fatal("wrong want-have count")
	}
	if len(pq0.wbs) != 0 {
		t.Fatal("wrong want-block count")
	}
	pq0.clear()

	// Send
	// - want-block cid 1 for session 1
	// - want-block cid 2 for session 1
	// - want-block cid 2 for session 2
	// - want-block cid 3 (new) for session 2
	cids2 := testutil.GenerateCids(1)
	pwm.sendWants(sid1, p0, cids1, nil)
	pwm.sendWants(sid2, p0, cids1[1:2], nil)
	pwm.sendWants(sid2, p0, cids2, nil)
	if len(pq0.whs) != 0 {
		t.Fatal("wrong want-have count")
	}

	// Two want-blocks should override existing want-haves for the same cids
	// One new want-block
	if len(pq0.wbs) != 3 {
		t.Fatal("wrong want-block count")
	}
}

func TestPWMMultiSessionSendWantOverrideBcst(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	// Broadcast
	// - want-have cid 1 for session 1
	// - want-have cid 2 for session 2
	cids1 := testutil.GenerateCids(2)
	pwm.broadcastWantHaves(sid1, cids1[:1])
	pwm.broadcastWantHaves(sid2, cids1[1:])
	pq0.clear()

	// Send
	// - want-have cid 1 for session 1
	// - want-have cid 1 for session 2
	// - want-block cid 2 for session 1
	// - want-block cid 2 for session 2
	pwm.sendWants(sid1, p0, nil, cids1[:1])
	pwm.sendWants(sid2, p0, nil, cids1[:1])
	pwm.sendWants(sid1, p0, cids1[1:], nil)
	pwm.sendWants(sid2, p0, cids1[1:], nil)

	// Broadcast already sent want-have for cid 1
	if len(pq0.whs) != 0 {
		t.Fatal("wrong want-have count")
	}

	// want-block should override broadcast want-have
	if len(pq0.wbs) != 1 {
		t.Fatal("wrong want-block count")
	}
}

func TestPWMSendCancels(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid := uint64(1)
	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	wb1 := testutil.GenerateCids(2)
	wh1 := testutil.GenerateCids(2)
	wb2 := testutil.GenerateCids(2)
	wh2 := testutil.GenerateCids(2)
	allwb := append(wb1, wb2...)
	allwh := append(wh1, wh2...)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		pwm.addPeer(pq, p)
	}
	pq0 := peerQueues[p0].(*mockPQ)
	pq1 := peerQueues[p1].(*mockPQ)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.sendWants(sid, p0, wb1, wh1)
	// Send 3 want-blocks and 3 want-haves to p1
	// (1 overlapping want-block / want-have with p0)
	pwm.sendWants(sid, p1, append(wb2, wb1[1]), append(wh2, wh1[1]))

	if !testutil.MatchKeysIgnoreOrder(pwm.getWantBlocks(), allwb) {
		t.Fatal("Expected 4 cids to be wanted")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.getWantHaves(), allwh) {
		t.Fatal("Expected 4 cids to be wanted")
	}

	// Cancel 1 want-block and 1 want-have that were sent to p0
	clearSent(peerQueues)
	pwm.sendCancels(sid, []cid.Cid{wb1[0], wh1[0]})
	// Should cancel the want-block and want-have
	if len(pq1.cancels) != 0 {
		t.Fatal("Expected no cancels sent to p1")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.cancels, []cid.Cid{wb1[0], wh1[0]}) {
		t.Fatal("Expected 2 cids to be cancelled")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.getWantBlocks(), append(wb2, wb1[1])) {
		t.Fatal("Expected 3 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.getWantHaves(), append(wh2, wh1[1])) {
		t.Fatal("Expected 3 want-haves")
	}

	// Cancel everything
	clearSent(peerQueues)
	allCids := append(allwb, allwh...)
	pwm.sendCancels(sid, allCids)
	// Should cancel the remaining want-blocks and want-haves for p0
	if !testutil.MatchKeysIgnoreOrder(pq0.cancels, []cid.Cid{wb1[1], wh1[1]}) {
		t.Fatal("Expected un-cancelled cids to be cancelled")
	}

	// Should cancel the remaining want-blocks and want-haves for p1
	remainingP1 := append(wb2, wh2...)
	remainingP1 = append(remainingP1, wb1[1], wh1[1])
	if len(pq1.cancels) != len(remainingP1) {
		t.Fatal("mismatch", len(pq1.cancels), len(remainingP1))
	}
	if !testutil.MatchKeysIgnoreOrder(pq1.cancels, remainingP1) {
		t.Fatal("Expected un-cancelled cids to be cancelled")
	}
	if len(pwm.getWantBlocks()) != 0 {
		t.Fatal("Expected 0 want-blocks")
	}
	if len(pwm.getWantHaves()) != 0 {
		t.Fatal("Expected 0 want-haves")
	}
}

func TestPWMMultiSessionCancelBcst(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	// Broadcast
	// cid0: s1, s2
	// cid1: s2
	cids1 := testutil.GenerateCids(2)
	pwm.broadcastWantHaves(sid1, cids1[:1])
	pwm.broadcastWantHaves(sid2, cids1)

	// Cancel both cids for session 2
	pwm.sendCancels(sid2, cids1)

	// Expect cancel to be sent only for cid with no remaining session
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent", len(pq0.cancels))
	}
	pq0.clear()

	// Cancel both cids for session 1
	pwm.sendCancels(sid1, cids1)

	// Expect cancel to be sent for remaining cid
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent")
	}
}

func TestPWMMultiSessionCancelWantHave(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	// want-have
	// cid0: s1, s2
	// cid1: s2
	cids1 := testutil.GenerateCids(2)
	pwm.sendWants(sid1, p0, nil, cids1[:1])
	pwm.sendWants(sid2, p0, nil, cids1)

	// Cancel both cids for session 2
	pwm.sendCancels(sid2, cids1)

	// Expect cancel to be sent only for cid with no remaining session
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent", len(pq0.cancels))
	}
	pq0.clear()

	// Cancel both cids for session 1
	pwm.sendCancels(sid1, cids1)

	// Expect cancel to be sent for remaining cid
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent")
	}
}

func TestPWMMultiSessionCancelWantBlock(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	// want-block
	// cid0: s1, s2
	// cid1: s2
	cids1 := testutil.GenerateCids(2)
	pwm.sendWants(sid1, p0, cids1[:1], nil)
	pwm.sendWants(sid2, p0, cids1, nil)

	// Cancel both cids for session 2
	pwm.sendCancels(sid2, cids1)

	// Expect cancel to be sent only for cid with no remaining session
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent", len(pq0.cancels))
	}
	pq0.clear()

	// Cancel both cids for session 1
	pwm.sendCancels(sid1, cids1)

	// Expect cancel to be sent for remaining cid
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent")
	}
}

func TestPWMMultiSessionCancelWantHaveAndBlock(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	cids1 := testutil.GenerateCids(1)
	// want-have cid0 s1
	pwm.sendWants(sid1, p0, nil, cids1)
	// want-block cid0 s2
	pwm.sendWants(sid2, p0, cids1, nil)

	// Cancel cid for session 2
	pwm.sendCancels(sid2, cids1)

	// Expect cancel not to be sent - there's still a want-have for cid0 from
	// session 1
	if len(pq0.cancels) != 0 {
		t.Fatal("wrong number of cancels sent")
	}

	// Cancel cid for session 2
	pwm.sendCancels(sid1, cids1)

	// Expect cancel to be sent now
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent")
	}
}

func TestPWMMultiSessionCancelBcstAndWantHave(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	cids1 := testutil.GenerateCids(1)
	// broadcast cid0 s1
	pwm.broadcastWantHaves(sid1, cids1)
	// want-have cid0 s2
	pwm.sendWants(sid2, p0, nil, cids1)

	// Cancel cid for session 2
	pwm.sendCancels(sid2, cids1)

	// Expect cancel not to be sent - there's still a broadcast for cid0 from
	// session 1
	if len(pq0.cancels) != 0 {
		t.Fatal("wrong number of cancels sent")
	}

	// Cancel cid for session 2
	pwm.sendCancels(sid1, cids1)

	// Expect cancel to be sent now
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent")
	}
}

func TestPWMMultiSessionCancelBcstAndWantBlock(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)

	p0 := peers[0]
	pq0 := &mockPQ{}

	// Add peer
	pwm.addPeer(pq0, p0)

	cids1 := testutil.GenerateCids(1)
	// broadcast cid0 s1
	pwm.broadcastWantHaves(sid1, cids1)
	// want-block cid0 s2
	pwm.sendWants(sid2, p0, cids1, nil)

	// Cancel cid for session 2
	pwm.sendCancels(sid2, cids1)

	// Expect cancel not to be sent - there's still a broadcast for cid0 from
	// session 1
	if len(pq0.cancels) != 0 {
		t.Fatal("wrong number of cancels sent")
	}

	// Cancel cid for session 1
	pwm.sendCancels(sid1, cids1)

	// Expect cancel to be sent now
	if len(pq0.cancels) != 1 {
		t.Fatal("wrong number of cancels sent")
	}
}

func TestPWMMultiSessionUnwanted(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	sid1 := uint64(1)
	sid2 := uint64(2)
	peers := testutil.GeneratePeers(1)
	cids := testutil.GenerateCids(2)
	cid0 := cids[:1]

	// Add peer
	p0 := peers[0]
	pq0 := &mockPQ{}
	pwm.addPeer(pq0, p0)

	if len(pwm.unwanted(cids)) != 2 {
		t.Fatal("wrong unwanted count")
	}

	// broadcast cid0 s1
	pwm.broadcastWantHaves(sid1, cid0)

	if len(pwm.unwanted(cids)) != 1 {
		t.Fatal("wrong unwanted count")
	}

	// want-block cid0 s2
	pwm.sendWants(sid2, p0, cid0, nil)

	if len(pwm.unwanted(cids)) != 1 {
		t.Fatal("wrong unwanted count")
	}

	// Cancel cid for session 2
	pwm.sendCancels(sid2, cid0)

	if len(pwm.unwanted(cids)) != 1 {
		t.Fatal("wrong unwanted count")
	}

	// Cancel cid for session 1
	pwm.sendCancels(sid1, cid0)

	if len(pwm.unwanted(cids)) != 2 {
		t.Fatal("wrong unwanted count")
	}
}

func TestStats(t *testing.T) {
	g := &gauge{}
	pwm := newPeerWantManager(g)

	sid := uint64(1)
	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	pq := &mockPQ{}
	peerQueues[p0] = pq
	pwm.addPeer(pq, p0)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.sendWants(sid, p0, cids, cids2)

	if g.count != 2 {
		t.Fatal("Expected 2 want-blocks")
	}

	// Send 1 old want-block and 2 new want-blocks to p0
	cids3 := testutil.GenerateCids(2)
	pwm.sendWants(sid, p0, append(cids3, cids[0]), []cid.Cid{})

	if g.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Cancel 1 want-block that was sent to p0
	// and 1 want-block that was not sent
	cids4 := testutil.GenerateCids(1)
	pwm.sendCancels(sid, append(cids4, cids[0]))

	if g.count != 3 {
		t.Fatal("Expected 3 want-blocks", g.count)
	}

	pwm.removePeer(p0)

	if g.count != 0 {
		t.Fatal("Expected all want-blocks to be removed with peer", g.count)
	}
}

func TestMultiSessionStats(t *testing.T) {
	g := &gauge{}
	pwm := newPeerWantManager(g)

	sid1 := uint64(1)
	sid2 := uint64(2)
	sid3 := uint64(3)
	peers := testutil.GeneratePeers(1)
	p0 := peers[0]
	pwm.addPeer(&mockPQ{}, p0)

	// Send
	// - want-have cid 1 for session 1
	// - want-have cid 2 for session 1
	// - want-have cid 2 for session 2
	cids1 := testutil.GenerateCids(2)
	pwm.sendWants(sid1, p0, nil, cids1)
	pwm.sendWants(sid2, p0, nil, cids1[1:2])
	if len(pwm.getWantHaves()) != 2 {
		t.Fatal("wrong want-have count")
	}
	if len(pwm.getWantBlocks()) != 0 {
		t.Fatal("wrong want-block count")
	}
	if len(pwm.getWants()) != 2 {
		t.Fatal("wrong want count")
	}
	if g.count != 0 {
		t.Fatal("wrong want-block guage count")
	}

	// Send
	// - want-block cid 3 for session 1
	// - want-block cid 4 for session 1
	// - want-block cid 4 for session 2
	cids2 := testutil.GenerateCids(2)
	pwm.sendWants(sid1, p0, cids2, nil)
	pwm.sendWants(sid2, p0, cids2[1:2], nil)
	if len(pwm.getWantHaves()) != 2 {
		t.Fatal("wrong want-have count")
	}
	if len(pwm.getWantBlocks()) != 2 {
		t.Fatal("wrong want-block count")
	}
	if len(pwm.getWants()) != 4 {
		t.Fatal("wrong want count")
	}
	if g.count != 2 {
		t.Fatal("wrong want-block guage count")
	}

	// Broadcast
	// - cid 1 for session 3
	// - cid 5 for session 3
	cids3 := testutil.GenerateCids(1)
	pwm.broadcastWantHaves(sid3, cids1[:1])
	pwm.broadcastWantHaves(sid3, cids3)
	if len(pwm.getWantHaves()) != 3 {
		t.Fatal("wrong want-have count")
	}
	if len(pwm.getWantBlocks()) != 2 {
		t.Fatal("wrong want-block count")
	}
	if len(pwm.getWants()) != 5 {
		t.Fatal("wrong want count")
	}
	if g.count != 2 {
		t.Fatal("wrong want-block guage count")
	}

	// Cancel first two want-have cids for session 1
	pwm.sendCancels(sid1, cids1)

	// There is still
	// cid1: a broadcast want (session 3)
	// cid2: a want-have (session 2)
	// so expect no change
	if len(pwm.getWantHaves()) != 3 {
		t.Fatal("wrong want-have count")
	}
	if len(pwm.getWantBlocks()) != 2 {
		t.Fatal("wrong want-block count")
	}
	if len(pwm.getWants()) != 5 {
		t.Fatal("wrong want count")
	}
	if g.count != 2 {
		t.Fatal("wrong want-block guage count")
	}

	// Cancel first want-have cid for session 3 (broadcast want)
	pwm.sendCancels(sid3, cids1[:1])

	if len(pwm.getWantHaves()) != 2 {
		t.Fatal("wrong want-have count")
	}
	if len(pwm.getWantBlocks()) != 2 {
		t.Fatal("wrong want-block count")
	}
	if len(pwm.getWants()) != 4 {
		t.Fatal("wrong want count")
	}
	if g.count != 2 {
		t.Fatal("wrong want-block guage count")
	}

	// Cancel third & fourth want-block cid for session 1
	pwm.sendCancels(sid1, cids2)

	// Should cancel third want-block but not fourth (session 2) still wants it
	if len(pwm.getWantHaves()) != 2 {
		t.Fatal("wrong want-have count")
	}
	if len(pwm.getWantBlocks()) != 1 {
		t.Fatal("wrong want-block count")
	}
	if len(pwm.getWants()) != 3 {
		t.Fatal("wrong want count")
	}
	if g.count != 1 {
		t.Fatal("wrong want-block guage count")
	}
}
