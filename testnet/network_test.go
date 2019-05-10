package bitswap

import (
	"context"
	"sync"
	"testing"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"

	bstestutil "github.com/ipfs/go-bitswap/testutil"
	blocks "github.com/ipfs/go-block-format"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	peer "github.com/libp2p/go-libp2p-peer"
	testutil "github.com/libp2p/go-testutil"
)

func TestSendMessageAsyncButWaitForResponse(t *testing.T) {
	net := VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0))
	responderPeer := testutil.RandIdentityOrFatal(t)
	waiter := net.Adapter(testutil.RandIdentityOrFatal(t))
	responder := net.Adapter(responderPeer)

	var wg sync.WaitGroup

	wg.Add(1)

	expectedStr := "received async"

	responder.SetDelegate(lambda(func(
		ctx context.Context,
		fromWaiter peer.ID,
		msgFromWaiter bsmsg.BitSwapMessage) {

		msgToWaiter := bsmsg.New(true)
		msgToWaiter.AddBlock(blocks.NewBlock([]byte(expectedStr)))
		waiter.SendMessage(ctx, fromWaiter, msgToWaiter)
	}))

	waiter.SetDelegate(lambda(func(
		ctx context.Context,
		fromResponder peer.ID,
		msgFromResponder bsmsg.BitSwapMessage) {

		// TODO assert that this came from the correct peer and that the message contents are as expected
		ok := false
		for _, b := range msgFromResponder.Blocks() {
			if string(b.RawData()) == expectedStr {
				wg.Done()
				ok = true
			}
		}

		if !ok {
			t.Fatal("Message not received from the responder")
		}
	}))

	messageSentAsync := bsmsg.New(true)
	messageSentAsync.AddBlock(blocks.NewBlock([]byte("data")))
	errSending := waiter.SendMessage(
		context.Background(), responderPeer.ID(), messageSentAsync)
	if errSending != nil {
		t.Fatal(errSending)
	}

	wg.Wait() // until waiter delegate function is executed
}

type fakeRateLimiter struct {
	rateLimits []float64
}

func (rl *fakeRateLimiter) NextRateLimit() float64 {
	next := rl.rateLimits[0]
	rl.rateLimits = rl.rateLimits[1:]
	return next
}
func TestRateLimiting(t *testing.T) {
	rateLimiter := &fakeRateLimiter{rateLimits: []float64{10000, 100000, 100000, 100000}}
	net := RateLimitedVirtualNetwork(mockrouting.NewServer(), delay.Fixed(0), rateLimiter)
	slowPeer := testutil.RandIdentityOrFatal(t)
	fastPeer := testutil.RandIdentityOrFatal(t)
	waiter := net.Adapter(testutil.RandIdentityOrFatal(t))
	slowResponder := net.Adapter(slowPeer)
	fastResponder := net.Adapter(fastPeer)

	var wg sync.WaitGroup

	wg.Add(2)

	makeResponderFunc := func(responder bsnet.BitSwapNetwork) bsnet.Receiver {
		return lambda(func(
			ctx context.Context,
			fromWaiter peer.ID,
			msgFromWaiter bsmsg.BitSwapMessage) {

			msgToWaiter := bsmsg.New(true)
			msgToWaiter.AddBlock(bstestutil.GenerateBlocksOfSize(1, 1000)[0])
			responder.SendMessage(ctx, fromWaiter, msgToWaiter)
		})
	}
	slowResponder.SetDelegate(makeResponderFunc(slowResponder))
	fastResponder.SetDelegate(makeResponderFunc(fastResponder))

	responseSenders := make([]peer.ID, 0, 2)
	waiter.SetDelegate(lambda(func(
		ctx context.Context,
		fromResponder peer.ID,
		msgFromResponder bsmsg.BitSwapMessage) {
		responseSenders = append(responseSenders, fromResponder)
		wg.Done()
	}))

	messageSentAsync := bsmsg.New(true)
	messageSentAsync.AddBlock(bstestutil.GenerateBlocksOfSize(1, 1000)[0])

	errSending := waiter.SendMessage(
		context.Background(), slowPeer.ID(), messageSentAsync)
	if errSending != nil {
		t.Fatal(errSending)
	}
	time.Sleep(5 * time.Millisecond)
	errSending = waiter.SendMessage(
		context.Background(), fastPeer.ID(), messageSentAsync)
	if errSending != nil {
		t.Fatal(errSending)
	}
	wg.Wait()

	if responseSenders[0] != fastPeer.ID() {
		t.Fatal("Fast peer should have responded first")
	}
	if responseSenders[1] != slowPeer.ID() {
		t.Fatal("Slow peer should have responded second")
	}

}

type receiverFunc func(ctx context.Context, p peer.ID,
	incoming bsmsg.BitSwapMessage)

// lambda returns a Receiver instance given a receiver function
func lambda(f receiverFunc) bsnet.Receiver {
	return &lambdaImpl{
		f: f,
	}
}

type lambdaImpl struct {
	f func(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage)
}

func (lam *lambdaImpl) ReceiveMessage(ctx context.Context,
	p peer.ID, incoming bsmsg.BitSwapMessage) {
	lam.f(ctx, p, incoming)
}

func (lam *lambdaImpl) ReceiveError(err error) {
	// TODO log error
}

func (lam *lambdaImpl) PeerConnected(p peer.ID) {
	// TODO
}
func (lam *lambdaImpl) PeerDisconnected(peer.ID) {
	// TODO
}
