package sessionpeermanager

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	cid "github.com/ipfs/go-cid"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

type fakePeerNetwork struct {
	peers       []peer.ID
	connManager ifconnmgr.ConnManager
}

func (fpn *fakePeerNetwork) ConnectionManager() ifconnmgr.ConnManager {
	return fpn.connManager
}

func (fpn *fakePeerNetwork) FindProvidersAsync(ctx context.Context, c cid.Cid, num int) <-chan peer.ID {
	peerCh := make(chan peer.ID)
	go func() {
		defer close(peerCh)
		for _, p := range fpn.peers {
			select {
			case peerCh <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	return peerCh
}

type fakeConnManager struct {
	taggedPeers []peer.ID
	wait        sync.WaitGroup
}

func (fcm *fakeConnManager) TagPeer(p peer.ID, tag string, n int) {
	fcm.wait.Add(1)
	fcm.taggedPeers = append(fcm.taggedPeers, p)
}

func (fcm *fakeConnManager) UntagPeer(p peer.ID, tag string) {
	defer fcm.wait.Done()

	for i := 0; i < len(fcm.taggedPeers); i++ {
		if fcm.taggedPeers[i] == p {
			fcm.taggedPeers[i] = fcm.taggedPeers[len(fcm.taggedPeers)-1]
			fcm.taggedPeers = fcm.taggedPeers[:len(fcm.taggedPeers)-1]
			return
		}
	}

}

func (*fakeConnManager) GetTagInfo(p peer.ID) *ifconnmgr.TagInfo { return nil }
func (*fakeConnManager) TrimOpenConns(ctx context.Context)       {}
func (*fakeConnManager) Notifee() inet.Notifiee                  { return nil }

func collectPeers(sessionPeerManager *SessionPeerManager) []peer.ID {
	keys := testutil.GenerateCids(maxOptimizedPeers)
	requests := sessionPeerManager.SplitRequestAmongPeers(keys)
	var sessionPeers []peer.ID
	for _, request := range requests {
		for _, peer := range request.Peers {
			if !testutil.ContainsPeer(sessionPeers, peer) {
				sessionPeers = append(sessionPeers, peer)
			}
		}
	}
	return sessionPeers
}
func TestFindingMorePeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	peers := testutil.GeneratePeers(5)
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpn)

	findCtx, findCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer findCancel()
	sessionPeerManager.FindMorePeers(ctx, c)
	<-findCtx.Done()
	sessionPeers := collectPeers(sessionPeerManager)
	if len(sessionPeers) != len(peers) {
		t.Fatal("incorrect number of peers found")
	}
	for _, p := range sessionPeers {
		if !testutil.ContainsPeer(peers, p) {
			t.Fatal("incorrect peer found through finding providers")
		}
	}
	if len(fcm.taggedPeers) != len(peers) {
		t.Fatal("Peers were not tagged!")
	}
}

func TestRecordingReceivedBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{nil, fcm}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpn)
	sessionPeerManager.RecordPeerResponse(p, c)
	time.Sleep(10 * time.Millisecond)
	sessionPeers := collectPeers(sessionPeerManager)
	if len(sessionPeers) != 1 {
		t.Fatal("did not add peer on receive")
	}
	if sessionPeers[0] != p {
		t.Fatal("incorrect peer added on receive")
	}
	if len(fcm.taggedPeers) != 1 {
		t.Fatal("Peers was not tagged!")
	}
}

func TestOrderingPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	peers := testutil.GeneratePeers(100)
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)
	id := testutil.GenerateSessionID()
	sessionPeerManager := New(ctx, id, fpn)

	// add all peers to session
	sessionPeerManager.FindMorePeers(ctx, c[0])

	// record broadcast
	sessionPeerManager.RecordPeerRequests(nil, c)

	// record receives
	peer1 := peers[rand.Intn(100)]
	peer2 := peers[rand.Intn(100)]
	peer3 := peers[rand.Intn(100)]
	time.Sleep(1 * time.Millisecond)
	// bring split down
	for i := 0; i < 5+minReceivedToAdjustSplit; i++ {
		sessionPeerManager.RecordPeerResponse(peer1, c[0])
	}
	time.Sleep(1 * time.Millisecond)
	sessionPeerManager.RecordPeerResponse(peer2, c[0])
	time.Sleep(1 * time.Millisecond)
	sessionPeerManager.RecordPeerResponse(peer3, c[0])

	ks := testutil.GenerateCids(1)
	requests := sessionPeerManager.SplitRequestAmongPeers(ks)
	sessionPeers := requests[0].Peers
	if len(requests[0].Peers) != maxOptimizedPeers {
		t.Fatal("Should not return more than the max of optimized peers")
	}

	// should prioritize peers which have received blocks
	if (sessionPeers[0] != peer3) || (sessionPeers[1] != peer2) || (sessionPeers[2] != peer1) {
		t.Fatal("Did not prioritize peers that received blocks")
	}

	// Receive a second time from same node
	sessionPeerManager.RecordPeerResponse(peer3, c[0])

	// call again
	requests = sessionPeerManager.SplitRequestAmongPeers(ks)
	nextSessionPeers := requests[0].Peers
	if len(nextSessionPeers) != maxOptimizedPeers {
		t.Fatal("Should not return more than the max of optimized peers")
	}

	// should not duplicate
	if (nextSessionPeers[0] != peer3) || (nextSessionPeers[1] != peer2) || (nextSessionPeers[2] != peer1) {
		t.Fatal("Did dedup peers which received multiple blocks")
	}

	// should randomize other peers
	totalSame := 0
	for i := 3; i < maxOptimizedPeers; i++ {
		if sessionPeers[i] == nextSessionPeers[i] {
			totalSame++
		}
	}
	if totalSame >= maxOptimizedPeers-3 {
		t.Fatal("should not return the same random peers each time")
	}
}
func TestUntaggingPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	peers := testutil.GeneratePeers(5)
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)
	if len(fcm.taggedPeers) != len(peers) {
		t.Fatal("Peers were not tagged!")
	}
	<-ctx.Done()
	fcm.wait.Wait()

	if len(fcm.taggedPeers) != 0 {
		t.Fatal("Peers were not untagged!")
	}
}

func TestSplittingRequests(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(10)
	keys := testutil.GenerateCids(6)
	id := testutil.GenerateSessionID()
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]

	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)

	partialRequests := sessionPeerManager.SplitRequestAmongPeers(keys)
	if len(partialRequests) != 2 {
		t.Fatal("Did not generate right number of partial requests")
	}
	for _, partialRequest := range partialRequests {
		if len(partialRequest.Peers) != 5 && len(partialRequest.Keys) != 3 {
			t.Fatal("Did not split request into even partial requests")
		}
	}
}

func TestSplittingRequestsTooFewKeys(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(10)
	keys := testutil.GenerateCids(1)
	id := testutil.GenerateSessionID()
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]
	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)

	partialRequests := sessionPeerManager.SplitRequestAmongPeers(keys)
	if len(partialRequests) != 1 {
		t.Fatal("Should only generate as many requests as keys")
	}
	for _, partialRequest := range partialRequests {
		if len(partialRequest.Peers) != 5 && len(partialRequest.Keys) != 1 {
			t.Fatal("Should still split peers up between keys")
		}
	}
}

func TestSplittingRequestsTooFewPeers(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(1)
	keys := testutil.GenerateCids(6)
	id := testutil.GenerateSessionID()
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]

	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)

	partialRequests := sessionPeerManager.SplitRequestAmongPeers(keys)
	if len(partialRequests) != 1 {
		t.Fatal("Should only generate as many requests as peers")
	}
	for _, partialRequest := range partialRequests {
		if len(partialRequest.Peers) != 1 && len(partialRequest.Keys) != 6 {
			t.Fatal("Should not split keys if there are not enough peers")
		}
	}
}

func TestSplittingRequestsIncreasingSplitDueToDupes(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(maxSplit)
	keys := testutil.GenerateCids(maxSplit)
	id := testutil.GenerateSessionID()
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]

	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < maxSplit+minReceivedToAdjustSplit; i++ {
		sessionPeerManager.RecordDuplicateBlock()
	}

	partialRequests := sessionPeerManager.SplitRequestAmongPeers(keys)
	if len(partialRequests) != maxSplit {
		t.Fatal("Did not adjust split up as duplicates came in")
	}
}

func TestSplittingRequestsDecreasingSplitDueToNoDupes(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(maxSplit)
	keys := testutil.GenerateCids(maxSplit)
	id := testutil.GenerateSessionID()
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]

	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 5+minReceivedToAdjustSplit; i++ {
		sessionPeerManager.RecordPeerResponse(peers[0], c)
	}

	partialRequests := sessionPeerManager.SplitRequestAmongPeers(keys)
	if len(partialRequests) != 1 {
		t.Fatal("Did not adjust split down as unique blocks came in")
	}
}
