package session

import (
	"testing"

	"github.com/ipfs/go-bitswap/testutil"
)

func TestEmptyWantPotential(t *testing.T) {
	cids := testutil.GenerateCids(1)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	if wp.Index() != 0 {
		t.Fatal("expected zero index")
	}
	if wp.bestPeer != "" {
		t.Fatal("expected no best peer")
	}
	if wp.bestPotential != 0 {
		t.Fatal("expected 0 best potential")
	}
	if wp.sentPotential != 0 {
		t.Fatal("expected 0 sent potential")
	}
}

func TestSetPeerPotential(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}
	if wp.sentPotential != 0 {
		t.Fatal("wrong sent potential")
	}

	wp.setPeerPotential(peers[1], 0.8)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.8 {
		t.Fatal("wrong best potential")
	}
	if wp.sentPotential != 0 {
		t.Fatal("wrong sent potential")
	}

	wp.setPeerPotential(peers[0], -0.8)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.8 {
		t.Fatal("wrong best potential")
	}
	if wp.sentPotential != 0 {
		t.Fatal("wrong sent potential")
	}
}

func TestSetPeerPotentialBestLower(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.8)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.8 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[1], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.8 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[0], -0.8)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}
}

func TestSetPeerPotentialNegative(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[1], -0.8)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}
}

func TestSetSamePeerPotential(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[0], 0.2)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.2 {
		t.Fatal("wrong best potential")
	}
}

func TestSetSamePeerPotentialZero(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[0], 0)
	if wp.bestPeer != "" {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0 {
		t.Fatal("wrong best potential")
	}
}

func TestSetSamePeerPotentialNegative(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[0], -0.8)
	if wp.bestPeer != "" {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0 {
		t.Fatal("wrong best potential")
	}
}

func TestClearThenSetNegative(t *testing.T) {
	cids := testutil.GenerateCids(1)
	peers := testutil.GeneratePeers(2)
	wp := newWantPotential(newPeerResponseTracker(), cids[0], 0)

	wp.setPeerPotential(peers[0], 0.5)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}

	gain := wp.clearPeerPotential(peers[0])
	if gain != 0.5 {
		t.Fatal("wrong gain")
	}
	if wp.bestPeer != "" {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[1], 0.5)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}

	wp.setPeerPotential(peers[0], -0.8)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
	if wp.bestPotential != 0.5 {
		t.Fatal("wrong best potential")
	}
}
