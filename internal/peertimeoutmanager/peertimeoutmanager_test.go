package peertimeoutmanager

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestPeerTimeoutManagerNoTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// Response received within timeout
	ptm.RequestSent(p)
	time.Sleep(time.Millisecond)
	ptm.ResponseReceived(p)

	<-tctx.Done()

	if len(timedOut) > 0 {
		t.Fatal("Expected request not to time out")
	}
}

func TestPeerTimeoutManagerWithTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// No response received within timeout
	ptm.RequestSent(p)

	<-tctx.Done()

	if len(timedOut) == 0 {
		t.Fatal("Expected request to time out")
	}
}

func TestPeerTimeoutManagerMultiRequestWithTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// Five requests sent every 2ms
	for i := 0; i < 5; i++ {
		ptm.RequestSent(p)
		time.Sleep(2 * time.Millisecond)
	}
	// Response received after 10ms (timeout is 5ms)
	ptm.ResponseReceived(p)

	<-tctx.Done()

	if len(timedOut) == 0 {
		t.Fatal("Expected request to time out")
	}
}

func TestPeerTimeoutManagerMultiRequestResponseWithTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// Response received within timeout
	ptm.RequestSent(p)
	time.Sleep(time.Millisecond)
	ptm.ResponseReceived(p)

	time.Sleep(time.Millisecond)

	// Another request sent but no response before timeout
	ptm.RequestSent(p)

	<-tctx.Done()

	if len(timedOut) == 0 {
		t.Fatal("Expected request to time out")
	}
}

func TestPeerTimeoutManagerMultiRequestResponseNoTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// Several requests and responses sent, all within timeout individually
	// but combined time more than timeout
	ptm.RequestSent(p)
	time.Sleep(time.Millisecond) //     +1 = 1
	ptm.ResponseReceived(p)

	time.Sleep(2 * time.Millisecond) // +2 = 3

	ptm.RequestSent(p)
	time.Sleep(time.Millisecond) //     +1 = 4
	ptm.ResponseReceived(p)

	time.Sleep(2 * time.Millisecond) // +2 = 6

	ptm.RequestSent(p)
	time.Sleep(time.Millisecond) //     +1 = 7
	ptm.ResponseReceived(p)

	<-tctx.Done()

	if len(timedOut) > 0 {
		t.Fatal("Expected request not to time out")
	}
}

func TestPeerTimeoutManagerWithSomePeersTimeout(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(2)
	p1 := peers[0]
	p2 := peers[1]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// Send request to p1 and p2 but only receive response from p1
	ptm.RequestSent(p1)
	ptm.RequestSent(p2)
	time.Sleep(time.Millisecond)
	ptm.ResponseReceived(p1)

	<-tctx.Done()

	if len(timedOut) != 1 {
		t.Fatal("Expected one peer request to time out")
	}
}

func TestPeerTimeoutManagerWithAllPeersTimeout(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(2)
	p1 := peers[0]
	p2 := peers[1]

	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 5*time.Millisecond)

	// Send request to p1 and p2 and get no response
	ptm.RequestSent(p1)
	ptm.RequestSent(p2)

	<-tctx.Done()

	if len(timedOut) != 2 {
		t.Fatal("Expected all peer requests to time out")
	}
}

// Launch a lot of requests and responses with some randomness and ensure they
// all time out correctly
func TestPeerTimeoutManagerWithManyRequests(t *testing.T) {
	ctx := context.Background()

	// Make sure we launch enough that the PeerTimeoutManager's internal
	// GC mechanism kicks in
	peers := testutil.GeneratePeers(requestGCCount * 3)

	var lk sync.Mutex
	var timedOut []peer.ID
	onTimeout := func(peers []peer.ID) {
		lk.Lock()
		defer lk.Unlock()
		timedOut = append(timedOut, peers...)
	}

	tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, onTimeout, 50*time.Millisecond)

	// Make batches of 5 peers
	expTimeout := int32(0)
	for i := 0; i < len(peers); i += 5 {
		end := i + 5
		if end > len(peers) {
			end = len(peers)
		}
		batch := peers[i:end]

		// Launch all batches concurrently
		go func() {
			// Add some random delay
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

			// Send all the requests
			var wgrq sync.WaitGroup
			for _, p := range batch {
				p := p
				wgrq.Add(1)
				go func() {
					defer wgrq.Done()
					ptm.RequestSent(p)
				}()
			}
			wgrq.Wait()

			// Sleep a little
			time.Sleep(5 * time.Millisecond)

			// Send responses for half of the requests (the rest should time
			// out)
			var wgrs sync.WaitGroup
			for i, p := range batch {
				if i%2 == 0 {
					p := p
					wgrs.Add(1)
					go func() {
						defer wgrs.Done()
						time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
						ptm.ResponseReceived(p)
					}()
				} else {
					atomic.AddInt32(&expTimeout, 1)
				}
			}
			wgrs.Wait()
		}()
	}

	<-tctx.Done()

	if len(timedOut) != int(expTimeout) {
		t.Fatal("Expected only some peer requests to time out", len(timedOut), expTimeout)
	}
}

func TestRemoveInactive(t *testing.T) {
	doTest := func(count int, active ...bool) {
		var rqs []*request
		for _, a := range active {
			rqs = append(rqs, &request{active: a})
		}

		after := removeInactive(rqs)
		if len(after) != count {
			t.Fatal(fmt.Sprintf("Expected %d requests after filter, got %d", count, len(after)))
		}
		for _, rq := range after {
			if !rq.active {
				t.Fatal("Expected all remaining requests to be active")
			}
		}
	}

	doTest(0)
	doTest(0, false)
	doTest(1, true)
	doTest(1, false, true)
	doTest(1, true, false)
	doTest(2, true, true)
	doTest(0, false, false)
	doTest(2, true, false, true)
	doTest(1, false, true, false)
	doTest(3, true, false, true, false, true)
	doTest(2, false, true, false, true, false)
	doTest(4, true, true, false, true, true)
	doTest(3, false, false, true, true, true)
}
