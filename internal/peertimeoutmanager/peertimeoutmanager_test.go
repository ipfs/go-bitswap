package peertimeoutmanager

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type timeoutRecorder struct {
	lk         sync.Mutex
	timedOut   []peer.ID
	timedOutCh chan peer.ID
}

func (tr *timeoutRecorder) onTimeout(peers []peer.ID) {
	tr.lk.Lock()
	defer tr.lk.Unlock()

	tr.timedOut = append(tr.timedOut, peers...)
	if tr.timedOutCh != nil {
		for _, p := range peers {
			tr.timedOutCh <- p
		}
	}
}

func (tr *timeoutRecorder) timedOutCount() int {
	tr.lk.Lock()
	defer tr.lk.Unlock()

	return len(tr.timedOut)
}

func TestPeerTimeoutManagerNoTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 10*time.Millisecond)

	// Response received within timeout
	ptm.RequestSent(p)
	time.Sleep(5 * time.Millisecond)
	ptm.ResponseReceived(p)

	<-tctx.Done()

	if tr.timedOutCount() > 0 {
		t.Fatal("Expected request not to time out")
	}
}

func TestPeerTimeoutManagerWithTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 5*time.Millisecond)

	// No response received within timeout
	ptm.RequestSent(p)

	<-tctx.Done()

	if tr.timedOutCount() == 0 {
		t.Fatal("Expected request to time out")
	}
}

func TestPeerTimeoutManagerMultiRequestWithTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 5*time.Millisecond)

	// Five requests sent every 2ms
	for i := 0; i < 5; i++ {
		ptm.RequestSent(p)
		time.Sleep(2 * time.Millisecond)
	}
	// Response received after 10ms (timeout is 5ms)
	ptm.ResponseReceived(p)

	<-tctx.Done()

	if tr.timedOutCount() == 0 {
		t.Fatal("Expected request to time out")
	}
}

func TestPeerTimeoutManagerMultiRequestResponseWithTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 10*time.Millisecond)

	// Response received within timeout
	ptm.RequestSent(p)
	time.Sleep(3 * time.Millisecond)
	ptm.ResponseReceived(p)

	time.Sleep(3 * time.Millisecond)

	// Another request sent but no response before timeout
	ptm.RequestSent(p)

	<-tctx.Done()

	if tr.timedOutCount() == 0 {
		t.Fatal("Expected request to time out")
	}
}

func TestPeerTimeoutManagerMultiRequestResponseNoTimeout(t *testing.T) {
	ctx := context.Background()
	p := testutil.GeneratePeers(1)[0]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 10*time.Millisecond)

	// Several requests and responses sent, all within timeout individually
	// but combined time more than timeout
	for i := 0; i < 7; i++ {
		ptm.RequestSent(p)
		time.Sleep(time.Millisecond) // +1ms
		ptm.ResponseReceived(p)

		time.Sleep(time.Millisecond) // +1ms
	}

	<-tctx.Done()

	if tr.timedOutCount() > 0 {
		t.Fatal("Expected request not to time out")
	}
}

func TestPeerTimeoutManagerWithSomePeersTimeout(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(2)
	p1 := peers[0]
	p2 := peers[1]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 10*time.Millisecond)

	// Send request to p1 and p2 but only receive response from p1
	ptm.RequestSent(p1)
	ptm.RequestSent(p2)
	time.Sleep(5 * time.Millisecond)
	ptm.ResponseReceived(p1)

	<-tctx.Done()

	if tr.timedOutCount() != 1 {
		t.Fatal("Expected one peer request to time out, got", tr.timedOutCount())
	}
}

func TestPeerTimeoutManagerWithAllPeersTimeout(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(2)
	p1 := peers[0]
	p2 := peers[1]
	tr := timeoutRecorder{}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	ptm := newPeerTimeoutManager(tctx, tr.onTimeout, 10*time.Millisecond)

	// Send request to p1 and p2 and get no response
	ptm.RequestSent(p1)
	ptm.RequestSent(p2)

	<-tctx.Done()

	if tr.timedOutCount() != 2 {
		t.Fatal("Expected all peer requests to time out")
	}
}

// Launch a lot of requests and responses with some randomness and ensure they
// all time out correctly
func TestPeerTimeoutManagerWithManyRequests(t *testing.T) {
	ctx := context.Background()
	tctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	// Make sure we launch enough requests that the PeerTimeoutManager's
	// internal GC mechanism kicks in
	peerCount := requestGCCount * 3
	timedOutCh := make(chan peer.ID, peerCount*2)
	tr := timeoutRecorder{timedOutCh: timedOutCh}

	peers := testutil.GeneratePeers(peerCount)

	ptm := newPeerTimeoutManager(ctx, tr.onTimeout, 100*time.Millisecond)

	// Make batches of 5 peers
	var lk sync.Mutex
	expTimeout := 0
	var allrqs sync.WaitGroup
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
			var grq sync.WaitGroup
			for _, p := range batch {
				p := p
				grq.Add(1)
				go func() {
					defer grq.Done()
					ptm.RequestSent(p)
				}()
			}
			grq.Wait()

			// Sleep a little
			time.Sleep(5 * time.Millisecond)

			// Send responses for half of the requests (the rest should time
			// out)
			for i, p := range batch {
				allrqs.Add(1)

				if i%2 == 0 {
					p := p
					go func() {
						defer allrqs.Done()

						time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
						ptm.ResponseReceived(p)
					}()
				} else {
					lk.Lock()
					expTimeout++
					lk.Unlock()

					allrqs.Done()
				}
			}
		}()
	}

	// Wait for the expected number of responses to time out
	timedOutCount := 0
	for {
		select {
		case <-timedOutCh:
			timedOutCount++

			lk.Lock()
			exp := expTimeout
			lk.Unlock()
			if timedOutCount == exp {
				return
			}
		case <-tctx.Done():
			// The test timed out before we got the expected number of timeouts
			t.Fatal(fmt.Sprintf("Expected %d peer requests to time out, but %d timed out", expTimeout, tr.timedOutCount()))
		}
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
