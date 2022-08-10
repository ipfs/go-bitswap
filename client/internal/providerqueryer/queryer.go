package providerqueryer

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/sync/semaphore"
)

const (
	maxConcurrency      = 6
	findProviderTimeout = 10 * time.Second
	maxProviders        = 10
	defaultTimeout      = 10 * time.Second
)

var log = logging.Logger("bitswap")

type queryNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID
}

type ProviderQueryer struct {
	clock   clock.Clock
	network queryNetwork
	sem     *semaphore.Weighted

	requests *requests
}

func New(network queryNetwork) *ProviderQueryer {
	return &ProviderQueryer{
		clock:   clock.New(),
		network: network,
		requests: &requests{
			m:    newRWMutex(),
			rMap: map[cid.Cid]*request{},
		},
		sem: semaphore.NewWeighted(maxConcurrency),
	}
}

// query is the main query function.
// Given multiple concurrent reqs for a CID, the first one runs this function
// and propagates results out to the others.
func (p *ProviderQueryer) query(ctx context.Context, req *request, k cid.Cid) {
	ctx, cancel := p.clock.WithTimeout(ctx, findProviderTimeout)
	defer cancel()

	defer req.Close()
	defer p.requests.RequestDone(k)

	select {
	case <-ctx.Done():
		return
	default:
	}

	providers := p.network.FindProvidersAsync(ctx, k, maxProviders)

	var wg sync.WaitGroup
	defer wg.Wait() // Wait for the notifications to finish.
	for {
		select {
		case <-ctx.Done():
			return
		case provider, ok := <-providers:
			if !ok {
				return
			}
			wg.Add(1)
			// Try to connect to the provider.
			// We only send it as a result if the connection succeeds.
			go func(prov peer.ID) {
				defer wg.Done()
				err := p.network.ConnectTo(ctx, prov)
				if err != nil {
					log.Debugf("failed to connect to provider %s: %s", p, err)
					return
				}
				// We can connect, notify all the receivers of the found provider.
				req.Notify(ctx, prov)
			}(provider)
		}
	}
}

func (p *ProviderQueryer) FindProvidersAsync(ctx context.Context, k cid.Cid) <-chan peer.ID {
	ch := make(chan peer.ID)

	go func() {
		isFirst, req := p.requests.AddReceiverOrCreateRequest(ctx, k, ch)
		if isFirst {
			err := p.sem.Acquire(ctx, 1)
			if err != nil {
				close(ch)
				return
			}
			defer p.sem.Release(1)
			p.query(ctx, req, k)
		}
	}()

	return ch
}
