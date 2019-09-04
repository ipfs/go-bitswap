package session

import (
	"testing"

	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestEmptySessionWants(t *testing.T) {
	sw := newSessionWants()
	cids := testutil.GenerateCids(1)

	// Expect these functions to return nothing on a new sessionWants
	lws := sw.PrepareBroadcast()
	if len(lws) > 0 {
		t.Fatal("expected no broadcast wants")
	}
	lws = sw.LiveWants()
	if len(lws) > 0 {
		t.Fatal("expected no live wants")
	}
	if sw.HasLiveWants() {
		t.Fatal("expected not to have live wants")
	}
	rw := sw.RandomLiveWant()
	if rw.Defined() {
		t.Fatal("expected no random want")
	}
	if sw.IsWanted(cids[0]) {
		t.Fatal("expected cid to not be wanted")
	}
	if len(sw.FilterInteresting(cids)) > 0 {
		t.Fatal("expected no interesting wants")
	}
}

func TestSessionWants(t *testing.T) {
	sw := newSessionWants()
	cids := testutil.GenerateCids(10)
	others := testutil.GenerateCids(1)

	// Add 10 new wants with a limit of 5
	// The first 5 cids should go into the toFetch queue
	// The other 5 cids should go into the live want queue
	//  toFetch   Live    Past
	//   98765    43210
	nextw := sw.GetNextWants(5, cids)
	if len(nextw) != 5 {
		t.Fatal("expected 5 next wants")
	}
	lws := sw.PrepareBroadcast()
	if len(lws) != 5 {
		t.Fatal("expected 5 broadcast wants", len(lws))
	}
	lws = sw.LiveWants()
	if len(lws) != 5 {
		t.Fatal("expected 5 live wants")
	}
	if !sw.HasLiveWants() {
		t.Fatal("expected to have live wants")
	}
	rw := sw.RandomLiveWant()
	if !rw.Defined() {
		t.Fatal("expected random want")
	}
	if !sw.IsWanted(cids[0]) {
		t.Fatal("expected cid to be wanted")
	}
	if !sw.IsWanted(cids[9]) {
		t.Fatal("expected cid to be wanted")
	}
	if len(sw.FilterInteresting([]cid.Cid{cids[0], cids[9], others[0]})) != 2 {
		t.Fatal("expected 2 interesting wants")
	}

	// Two wanted blocks and one other block are received.
	// The wanted blocks should be moved from the live wants queue
	// to the past wants set (the other block CID should be ignored)
	//  toFetch   Live    Past
	//   98765    432__   10
	recvdCids := []cid.Cid{cids[0], cids[1], others[0]}
	uniq := 0
	dup := 0
	sw.ForEachUniqDup(recvdCids, func() { uniq++ }, func() { dup++ })
	if uniq != 2 || dup != 0 {
		t.Fatal("expected 2 uniqs / 0 dups", uniq, dup)
	}
	sw.BlocksReceived(recvdCids)
	lws = sw.LiveWants()
	if len(lws) != 3 {
		t.Fatal("expected 3 live wants")
	}
	if sw.IsWanted(cids[0]) {
		t.Fatal("expected cid to no longer be wanted")
	}
	if !sw.IsWanted(cids[9]) {
		t.Fatal("expected cid to be wanted")
	}
	if len(sw.FilterInteresting([]cid.Cid{cids[0], cids[9], others[0]})) != 2 {
		t.Fatal("expected 2 interesting wants")
	}

	// Ask for next wants with a limit of 5
	// Should move 2 wants from toFetch queue to live wants
	//  toFetch   Live    Past
	//   987__    65432   10
	nextw = sw.GetNextWants(5, nil)
	if len(nextw) != 2 {
		t.Fatal("expected 2 next wants")
	}
	lws = sw.LiveWants()
	if len(lws) != 5 {
		t.Fatal("expected 5 live wants")
	}
	if !sw.IsWanted(cids[5]) {
		t.Fatal("expected cid to be wanted")
	}

	// One wanted block and one dup block are received.
	// The wanted block should be moved from the live wants queue
	// to the past wants set
	//  toFetch   Live    Past
	//   987      654_2   310
	recvdCids = []cid.Cid{cids[0], cids[3]}
	uniq = 0
	dup = 0
	sw.ForEachUniqDup(recvdCids, func() { uniq++ }, func() { dup++ })
	if uniq != 1 || dup != 1 {
		t.Fatal("expected 1 uniq / 1 dup", uniq, dup)
	}
	sw.BlocksReceived(recvdCids)
	lws = sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}

	// One block in the toFetch queue should be cancelled
	//  toFetch   Live    Past
	//   9_7      654_2   310
	sw.CancelPending([]cid.Cid{cids[8]})
	lws = sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}
	if sw.IsWanted(cids[8]) {
		t.Fatal("expected cid to no longer be wanted")
	}
	if len(sw.FilterInteresting([]cid.Cid{cids[0], cids[8]})) != 1 {
		t.Fatal("expected 1 interesting wants")
	}
}

func TestPopNextPending(t *testing.T) {
	sw := newSessionWants()
	cids := testutil.GenerateCids(10)
	// others := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(3)

	// Add 10 new wants with a limit of 5
	// The first 5 cids should go into the live want queue
	// The other 5 cids should go into the toFetch queue
	//  toFetch   Live    Past
	//   98765    43210
	nextw := sw.GetNextWants(5, cids)
	if len(nextw) != 5 {
		t.Fatal("expected 5 next wants")
	}
	if sw.liveWants.wasWantBlockSentToPeer(cids[0], peers[0]) {
		t.Fatal("expected no want to have been sent to peer")
	}

	// Pop best want
	// - c1 should be one of the live cids (random)
	// - wantHaves should be all cids except c1
	// - p1 should be one of the peers (random)
	// - ph should be all peers except p1
log.Warningf("Pop1")
	c1, wantHaves, p1, ph := sw.PopNextPending(peers)
	if p1 == "" {
		t.Fatal("expected peer")
	}
	if !c1.Defined() {
		t.Fatal("expected cid")
	}
	if len(wantHaves) != 9 {
		t.Fatal("expected 9 wantHaves", len(wantHaves))
	}
	for _, c := range cids {
		if c == c1 {
			if sw.liveWants.wasWantHaveSentToPeer(c, p1) {
				t.Fatal("expected want-have c1 not to have been sent to p")
			}
		} else {
			if !sw.liveWants.wasWantHaveSentToPeer(c, p1) {
				t.Fatal("expected want-have c to have been sent to p")
			}
		}
	}
	if len(ph) != 2 {
		t.Fatal("expected 2 peerHaves")
	}

	if sw.liveWants.sumWantPotential(c1) != 0.5 {
		t.Fatal("expected want potential 0.5")
	}
	if !sw.liveWants.wasWantBlockSentToPeer(c1, p1) {
		t.Fatal("expected want to have been sent to peer")
	}
	notSentPeers := sw.liveWants.wantBlockNotSentToPeers(c1, peers)
	if len(notSentPeers) != 2 {
		t.Fatal("expected want to not have been sent to other 2 peers")
	}
	for _, nsp := range notSentPeers {
		if nsp == p1 {
			t.Fatal("expected not sent peers to exclude returned peer")
		}
	}
	if sw.liveWants.receivedDontHaveFromPeer(c1, p1) {
		t.Fatal("expected not to have received DONT_HAVE")
	}


	otherPeers := make([]peer.ID, 0, 2)
	for _, p := range peers {
		if p != p1 {
			otherPeers = append(otherPeers, p)
		}
	}
	// Pop next best want with one of the other two peers
	// - c2 should be one of the live cids except c1
	// - wantHaves should be all cids except c2
	// - p2 should be one of otherPeers (random)
	// - ph should not be p2
log.Warningf("Pop2")
	c2, wantHaves2, p2, ph2 := sw.PopNextPending(otherPeers)
log.Warningf("Pop2 done")
	if p2 == "" {
		t.Fatal("expected peer")
	}
	if !c2.Defined() {
		t.Fatal("expected cid")
	}
	if c2.Equals(c1) {
		t.Fatal("expected different cid")
	}
	if len(wantHaves2) != 9 {
		t.Fatal("expected 9 wantHaves", len(wantHaves2))
	}
	if len(ph2) != 1 {
		t.Fatal("expected 1 peerHaves")
	}
	if ph2[0] == p2 {
		t.Fatal("expected different peer")
	}

	if sw.liveWants.sumWantPotential(c2) != 0.5 {
		t.Fatal("expected want potential 0.5")
	}
	if !sw.liveWants.wasWantBlockSentToPeer(c2, p2) {
		t.Fatal("expected want to have been sent to peer")
	}
	notSentPeers = sw.liveWants.wantBlockNotSentToPeers(c2, peers)
	if len(notSentPeers) != 2 {
		t.Fatal("expected want to not have been sent to other 2 peers")
	}
	for _, nsp := range notSentPeers {
		if nsp == p2 {
			t.Fatal("expected not sent peers to exclude returned peer")
		}
	}

	// Receive DONT_HAVE for first cid
	sw.BlockInfoReceived(p1, []cid.Cid{}, []cid.Cid{c1})
	if sw.liveWants.sumWantPotential(c1) != 0.0 {
		t.Fatal("expected want potential 0")
	}
	if !sw.liveWants.wasWantBlockSentToPeer(c1, p1) {
		t.Fatal("expected want to have been sent to peer")
	}
	notSentPeers = sw.liveWants.wantBlockNotSentToPeers(c1, peers)
	if len(notSentPeers) != 2 {
		t.Fatal("expected want to not have been sent to other 2 peers")
	}
	for _, nsp := range notSentPeers {
		if nsp == p1 {
			t.Fatal("expected not sent peers to exclude returned peer")
		}
	}
	if !sw.liveWants.receivedDontHaveFromPeer(c1, p1) {
		t.Fatal("expected to have received DONT_HAVE")
	}


	// Receive block for second cid
	// One CID should have moved from live to past list
	//  toFetch   Live    Past
	//   98765    43_10     2
	wanted, _ := sw.BlocksReceived([]cid.Cid{c2})
	if len(wanted) != 1 {
		t.Fatal("expected 1 wanted cid")
	}
	if !wanted[0].Equals(c2) {
		t.Fatal("expected cid to be wanted")
	}
	if sw.liveWants.sumWantPotential(c2) != 0.0 {
		t.Fatal("expected want potential 0")
	}
	lws := sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}
}
