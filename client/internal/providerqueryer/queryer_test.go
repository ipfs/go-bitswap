package providerqueryer

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-bitswap/internal/testutil"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type fakeProviderNetwork struct {
	clock            clock.Clock
	peersFound       []peer.ID
	connectError     error
	delay            time.Duration
	connectDelay     time.Duration
	queriesMadeMutex sync.RWMutex
	queriesMade      int
	liveQueries      int
}

func (fpn *fakeProviderNetwork) ConnectTo(ctx context.Context, p peer.ID) error {
	fpn.clock.Sleep(fpn.connectDelay)
	return fpn.connectError
}

func (fpn *fakeProviderNetwork) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.ID {
	fpn.queriesMadeMutex.Lock()
	fpn.queriesMade++
	fpn.liveQueries++
	fpn.queriesMadeMutex.Unlock()
	incomingPeers := make(chan peer.ID)
	go func() {
		defer close(incomingPeers)
		for _, p := range fpn.peersFound {
			fpn.clock.Sleep(fpn.delay)
			select {
			case incomingPeers <- p:
			case <-ctx.Done():
				return
			}
		}
		fpn.queriesMadeMutex.Lock()
		fpn.liveQueries--
		fpn.queriesMadeMutex.Unlock()
	}()

	return incomingPeers
}

// reqBuf makes a request and buffers the results into a slice.
type reqBuf struct {
	m       sync.Mutex
	key     cid.Cid
	started bool
	peers   []peer.ID
	wg      sync.WaitGroup
}

func (b *reqBuf) run(f func() <-chan peer.ID) {
	b.wg.Add(1)

	go func() {
		defer b.wg.Done()

		ch := f()
		b.m.Lock()
		b.started = true
		b.m.Unlock()

		for p := range ch {
			b.m.Lock()
			b.peers = append(b.peers, p)
			b.m.Unlock()
		}
	}()
}

func (b *reqBuf) getPeers() []peer.ID {
	b.m.Lock()
	defer b.m.Unlock()
	peers := make([]peer.ID, len(b.peers))
	copy(peers, b.peers)
	return peers
}

func (b *reqBuf) wait() {
	b.wg.Wait()
}

func TestQueryer(t *testing.T) {
	type req struct {
		timeout time.Duration
		keyIdx  int // these are indices in the generated key slice
	}

	type reqState struct {
		numProvsFound int
	}

	// each epoch is a period of time we advance the clock before asserting the expected state of the requests
	type epoch struct {
		advance   time.Duration // time to wait before applying this epoch's assertions
		expStates []reqState
	}

	cases := []struct {
		name              string
		numProviders      int
		numKeys           int
		provDelay         time.Duration // time to wait between sending each provider result from the mock provider network
		cancelCtx         bool
		findProvTimeout   time.Duration
		connErrs          bool
		maxConcurrentReqs int64
		reqs              []req
		epochs            []epoch
		expQueries        int
	}{
		{
			name:         "normal simultaneous fetch",
			numProviders: 10,
			numKeys:      2,
			reqs: []req{
				{keyIdx: 0},
				{keyIdx: 1},
			},
			epochs: []epoch{
				{
					expStates: []reqState{
						{numProvsFound: 10},
						{numProvsFound: 10},
					},
				},
			},
			expQueries: 2,
		},
		{
			name:         "concurrent requests for same key should only result in a single query",
			numProviders: 10,
			numKeys:      1,
			reqs: []req{
				{keyIdx: 0},
				{keyIdx: 0},
			},
			epochs: []epoch{
				{
					expStates: []reqState{
						{numProvsFound: 10},
						{numProvsFound: 10},
					},
				},
			},
			expQueries: 1,
		},
		{
			name:         "canceling one request doesn't cancel another",
			numProviders: 10,
			numKeys:      2,
			provDelay:    6 * time.Millisecond,
			reqs: []req{
				{timeout: 10 * time.Millisecond, keyIdx: 0},
				{timeout: 20 * time.Millisecond, keyIdx: 1},
			},
			epochs: []epoch{
				{
					advance: 15 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 1},
						{numProvsFound: 2},
					},
				},
				{
					advance: 15 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 1},
						{numProvsFound: 3},
					},
				},
			},
			expQueries: 2,
		},
		{
			name:         "unconnectable providers aren't sent as results",
			numProviders: 10,
			numKeys:      1,
			connErrs:     true,
			reqs: []req{
				{keyIdx: 0},
				{keyIdx: 0},
			},
			epochs: []epoch{
				{
					expStates: []reqState{
						{numProvsFound: 0},
						{numProvsFound: 0},
					},
				},
			},
			expQueries: 1,
		},
		{
			name:              "maximum concurrent requests are respected",
			numProviders:      2,
			numKeys:           2,
			provDelay:         1 * time.Millisecond,
			maxConcurrentReqs: 1,
			reqs: []req{
				{keyIdx: 0},
				{keyIdx: 1},
			},
			epochs: []epoch{
				{
					advance: 0,
					expStates: []reqState{
						{numProvsFound: 0},
						{numProvsFound: 0},
					},
				},
				{
					advance: 1 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 1},
						{numProvsFound: 0},
					},
				},
				{
					advance: 1 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 2},
						{numProvsFound: 0},
					},
				},
				{
					advance: 1 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 2},
						{numProvsFound: 1},
					},
				},
				{
					advance: 1 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 2},
						{numProvsFound: 2},
					},
				},
			},
			expQueries: 2,
		},
		{
			name:         "find provider timeout should be respected",
			numProviders: 1,
			numKeys:      1,
			provDelay:    findProviderTimeout + 100*time.Millisecond,
			reqs: []req{
				{keyIdx: 0},
			},
			epochs: []epoch{
				{
					advance: 1 * time.Millisecond,
					expStates: []reqState{
						{numProvsFound: 0},
					},
				},
				{
					advance: findProviderTimeout,
					expStates: []reqState{
						{numProvsFound: 0},
					},
				},
			},
			expQueries: 1,
		},
		{
			name:         "query completes even if the context is already canceled",
			numProviders: 1,
			numKeys:      1,
			provDelay:    1 * time.Millisecond,
			cancelCtx:    true,
			reqs: []req{
				{keyIdx: 0},
			},
			epochs: []epoch{
				{
					advance: 0,
					expStates: []reqState{
						{numProvsFound: 0},
					},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			peers := testutil.GeneratePeers(c.numProviders)
			mockClock := clock.NewMock()
			fpn := &fakeProviderNetwork{
				clock:      mockClock,
				peersFound: peers,
				delay:      c.provDelay,
			}

			if c.connErrs {
				fpn.connectError = errors.New("unable to connect")
			}

			ctx := context.Background()
			ctx, cancel := mockClock.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if c.cancelCtx {
				cancel()
			}

			queryer := New(fpn)
			queryer.clock = mockClock

			if c.maxConcurrentReqs != 0 {
				queryer.sem = semaphore.NewWeighted(c.maxConcurrentReqs)
			}

			keys := testutil.GenerateCids(c.numKeys)

			var bufs []*reqBuf
			for _, req := range c.reqs {
				reqCtx := ctx
				var reqCancel func()
				if req.timeout > 0 {
					reqCtx, reqCancel = mockClock.WithTimeout(reqCtx, req.timeout)
					defer reqCancel()
				}
				k := keys[req.keyIdx]
				buf := &reqBuf{key: k}
				buf.run(func() <-chan peer.ID {
					return queryer.FindProvidersAsync(reqCtx, k)
				})
				bufs = append(bufs, buf)
				// fire clock events
				mockClock.Add(0)
			}

			for epochIdx, epoch := range c.epochs {
				mockClock.Add(epoch.advance)
				for reqIdx, reqState := range epoch.expStates {
					buf := bufs[reqIdx]
					assert.Equal(t, reqState.numProvsFound, len(buf.getPeers()), "unexpected number of results in epoch=%d for req=%d", epochIdx, reqIdx)
				}
			}

			for _, buf := range bufs {
				buf.wait()
			}

			fpn.queriesMadeMutex.Lock()
			defer fpn.queriesMadeMutex.Unlock()
			assert.Equal(t, c.expQueries, fpn.queriesMade, "didn't make expected number of downstream queries")
		})
	}

}

func TestQueryer_Fuzz(t *testing.T) {
	// Fuzz the queryer to check for races, deadlocks, etc.
	//
	// We don't use Go's fuzz testing framework because it constantly craps out
	// due to an unconfigurable per-test timeout. Once that's fixed we can probably
	// switch to it.

	defer goleak.VerifyNone(
		t,
		goleak.IgnoreTopFunction("github.com/ipfs/go-log/writer.(*MirrorWriter).logRoutine"),
		goleak.IgnoreTopFunction("github.com/benbjohnson/clock.(*Mock).Sleep"), // from fakeProviderNetwork
	)

	deadlock.Opts.DeadlockTimeout = 1 * time.Second

	// switch to using instrumented locks to detect deadlocks
	origNewMutex := newMutex
	origNewRWMutex := newRWMutex
	newMutex = func() sync.Locker { return &deadlock.Mutex{} }
	newRWMutex = func() rwLocker { return &deadlock.RWMutex{} }
	defer func() { newMutex = origNewMutex }()
	defer func() { newRWMutex = origNewRWMutex }()

	timesRun := 0
	for i := 0; i < 100; i++ {
		peers := rand.Int()
		if peers < 0 {
			peers = -peers
		}
		peers %= 10

		provs := testutil.GeneratePeers(peers)

		provSet := map[peer.ID]bool{}
		for _, p := range provs {
			provSet[p] = true
		}

		fpn := &fakeProviderNetwork{
			clock:      clock.New(),
			peersFound: provs,
		}
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		queryer := New(fpn)

		// keeping this low increases the probability of concurrent cid requests
		cids := testutil.GenerateCids(2)

		results := make(chan map[peer.ID]bool)
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			// pick some random cid from the set to query
			c := cids[rand.Int()%len(cids)]
			wg.Add(1)
			timesRun++
			go func(c cid.Cid) {
				defer wg.Done()

				buf := &reqBuf{key: c}
				buf.run(func() <-chan peer.ID {
					return queryer.FindProvidersAsync(ctx, c)
				})
				buf.wait()

				receivedProvsSet := map[peer.ID]bool{}
				for _, p := range buf.getPeers() {
					receivedProvsSet[p] = true
				}
				results <- receivedProvsSet

			}(c)
		}

		go func() {
			wg.Wait()
			close(results)
		}()

	LOOP:
		for {
			select {
			case <-ctx.Done():
				t.Fatalf("timeout")
			case r, ok := <-results:
				if !ok {
					break LOOP
				}
				require.Equal(t, provSet, r)
			}
		}
	}
	t.Logf("fuzzer ran %d times", timesRun)

	// give
}
