// Package bitswap implements the IPFS exchange interface with the BitSwap
// bilateral exchange protocol.
package bitswap

import (
	"context"
	"fmt"

	"sync"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	deciface "github.com/ipfs/go-bitswap/decision"
	"github.com/ipfs/go-bitswap/internal"
	"github.com/ipfs/go-bitswap/internal/decision"
	"github.com/ipfs/go-bitswap/internal/defaults"
	bsgetter "github.com/ipfs/go-bitswap/internal/getter"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-metrics-interface"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")
var sflog = log.Desugar()

var _ exchange.SessionExchange = (*Bitswap)(nil)

var (
	// the 1<<18+15 is to observe old file chunks that are 1<<18 + 14 in size
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}

	timeMetricsBuckets = []float64{1, 10, 30, 60, 90, 120, 600}
)

// Option defines the functional option type that can be used to configure
// bitswap instances
type Option func(*Bitswap)

// ProvideEnabled is an option for enabling/disabling provide announcements
func ProvideEnabled(enabled bool) Option {
	return func(bs *Bitswap) {
		bs.provideEnabled = enabled
	}
}

// ProviderSearchDelay overwrites the global provider search delay
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return func(bs *Bitswap) {
		bs.provSearchDelay = newProvSearchDelay
	}
}

// RebroadcastDelay overwrites the global provider rebroadcast delay
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return func(bs *Bitswap) {
		bs.rebroadcastDelay = newRebroadcastDelay
	}
}

// EngineBlockstoreWorkerCount sets the number of worker threads used for
// blockstore operations in the decision engine
func EngineBlockstoreWorkerCount(count int) Option {
	if count <= 0 {
		panic(fmt.Sprintf("Engine blockstore worker count is %d but must be > 0", count))
	}
	return func(bs *Bitswap) {
		bs.engineBstoreWorkerCount = count
	}
}

// EngineTaskWorkerCount sets the number of worker threads used inside the engine
func EngineTaskWorkerCount(count int) Option {
	if count <= 0 {
		panic(fmt.Sprintf("Engine task worker count is %d but must be > 0", count))
	}
	return func(bs *Bitswap) {
		bs.engineTaskWorkerCount = count
	}
}

func TaskWorkerCount(count int) Option {
	if count <= 0 {
		panic(fmt.Sprintf("task worker count is %d but must be > 0", count))
	}
	return func(bs *Bitswap) {
		bs.taskWorkerCount = count
	}
}

// MaxOutstandingBytesPerPeer describes approximately how much work we are will to have outstanding to a peer at any
// given time. Setting it to 0 will disable any limiting.
func MaxOutstandingBytesPerPeer(count int) Option {
	if count < 0 {
		panic(fmt.Sprintf("max outstanding bytes per peer is %d but must be >= 0", count))
	}
	return func(bs *Bitswap) {
		bs.engineMaxOutstandingBytesPerPeer = count
	}
}

// SetSendDontHaves indicates what to do when the engine receives a want-block
// for a block that is not in the blockstore. Either
// - Send a DONT_HAVE message
// - Simply don't respond
// This option is only used for testing.
func SetSendDontHaves(send bool) Option {
	return func(bs *Bitswap) {
		bs.engineSetSendDontHaves = send
	}
}

// Configures the engine to use the given score decision logic.
func WithScoreLedger(scoreLedger deciface.ScoreLedger) Option {
	return func(bs *Bitswap) {
		bs.engineScoreLedger = scoreLedger
	}
}

func SetSimulateDontHavesOnTimeout(send bool) Option {
	return func(bs *Bitswap) {
		bs.simulateDontHavesOnTimeout = send
	}
}

func WithTargetMessageSize(tms int) Option {
	return func(bs *Bitswap) {
		bs.engineTargetMessageSize = tms
	}
}

func WithPeerBlockRequestFilter(pbrf PeerBlockRequestFilter) Option {
	return func(bs *Bitswap) {
		bs.peerBlockRequestFilter = pbrf
	}
}

type Server interface {
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
	ReceiveMessage(context.Context, peer.ID, bsmsg.BitSwapMessage)
	ReceivedBlocks(context.Context, peer.ID, []blocks.Block)
	WantlistForPeer(peer.ID) []cid.Cid
	LedgerForPeer(peer.ID) *decision.Receipt
	Peers() []peer.ID
}

func WithServer(s Server) Option {
	return func(bs *Bitswap) {
		bs.server = s
	}
}

type Client interface {
	exchange.Fetcher
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
	NewSession(ctx context.Context) exchange.Fetcher
	ReceiveMessage(context.Context, peer.ID, bsmsg.BitSwapMessage)
	ReceivedBlocksFrom(ctx context.Context, p peer.ID, blks []blocks.Block, haves []cid.Cid, dontHaves []cid.Cid) error
	GetWantlist() []cid.Cid
	GetWantBlocks() []cid.Cid
	GetWantHaves() []cid.Cid
}

func WithClient(c Client) Option {
	return func(bs *Bitswap) {
		bs.client = c
	}
}

type TaskInfo = decision.TaskInfo
type TaskComparator = decision.TaskComparator
type PeerBlockRequestFilter = decision.PeerBlockRequestFilter

// WithTaskComparator configures custom task prioritization logic.
func WithTaskComparator(comparator TaskComparator) Option {
	return func(bs *Bitswap) {
		bs.taskComparator = comparator
	}
}

// New initializes a BitSwap instance that communicates over the provided
// BitSwapNetwork. This function registers the returned instance as the network
// delegate. Runs until context is cancelled or bitswap.Close is called.
func New(parent context.Context, network bsnet.BitSwapNetwork,
	bstore blockstore.Blockstore, options ...Option) exchange.Interface {

	// important to use provided parent context (since it may include important
	// loggable data). It's probably not a good idea to allow bitswap to be
	// coupled to the concerns of the ipfs daemon in this way.
	//
	// FIXME(btc) Now that bitswap manages itself using a process, it probably
	// shouldn't accept a context anymore. Clients should probably use Close()
	// exclusively. We should probably find another way to share logging data
	ctx, cancelFunc := context.WithCancel(parent)
	ctx = metrics.CtxSubScope(ctx, "bitswap")
	dupHist := metrics.NewCtx(ctx, "recv_dup_blocks_bytes", "Summary of duplicate"+
		" data blocks recived").Histogram(metricsBuckets)
	allHist := metrics.NewCtx(ctx, "recv_all_blocks_bytes", "Summary of all"+
		" data blocks recived").Histogram(metricsBuckets)

	sentHistogram := metrics.NewCtx(ctx, "sent_all_blocks_bytes", "Histogram of blocks sent by"+
		" this bitswap").Histogram(metricsBuckets)

	sendTimeHistogram := metrics.NewCtx(ctx, "send_times", "Histogram of how long it takes to send messages"+
		" in this bitswap").Histogram(timeMetricsBuckets)

	px := process.WithTeardown(func() error {
		return nil
	})

	bs := &Bitswap{
		blockstore:                       bstore,
		network:                          network,
		process:                          px,
		counters:                         new(counters),
		dupMetric:                        dupHist,
		allMetric:                        allHist,
		sentHistogram:                    sentHistogram,
		sendTimeHistogram:                sendTimeHistogram,
		provideEnabled:                   true,
		provSearchDelay:                  defaults.ProvSearchDelay,
		rebroadcastDelay:                 delay.Fixed(time.Minute),
		engineBstoreWorkerCount:          defaults.BitswapEngineBlockstoreWorkerCount,
		engineTaskWorkerCount:            defaults.BitswapEngineTaskWorkerCount,
		taskWorkerCount:                  defaults.BitswapTaskWorkerCount,
		engineMaxOutstandingBytesPerPeer: defaults.BitswapMaxOutstandingBytesPerPeer,
		engineTargetMessageSize:          defaults.BitswapEngineTargetMessageSize,
		engineSetSendDontHaves:           true,
		simulateDontHavesOnTimeout:       true,
	}

	// apply functional options before starting and running bitswap
	for _, option := range options {
		option(bs)
	}

	if bs.provideEnabled {
		bs.provider = newProvider()
	}

	if bs.server == nil {
		bs.server = NewServer(
			ctx,
			px,
			bstore,
			network,
			bs.provider.Provide,
			WithServerPeerBlockRequestFilter(bs.peerBlockRequestFilter),
			WithServerSetSendDontHaves(bs.engineSetSendDontHaves),
			WithServerTargetMessageSize(bs.engineTargetMessageSize),
			WithServerTaskComparator(bs.taskComparator),
			WithServerEngineBlockstoreWorkerCount(bs.engineBstoreWorkerCount),
			WithServerEngineTaskWorkerCount(bs.engineTaskWorkerCount),
			WithServerMaxOutstandingBytesPerPeer(bs.engineMaxOutstandingBytesPerPeer),
		)
	}

	if bs.client == nil {
		bs.client = NewClient(
			px,
			bstore,
			network,
			bs.server.ReceivedBlocks,
			WithClientProviderSearchDelay(bs.provSearchDelay),
			WithClientRebroadcastDelay(bs.rebroadcastDelay),
		)
	}

	network.Start(bs)

	// Start up bitswaps async worker routines
	bs.startWorkers(ctx, px)

	// bind the context and process.
	// do it over here to avoid closing before all setup is done.
	go func() {
		<-px.Closing() // process closes first
		bs.provider.Shutdown()
		cancelFunc()
		network.Stop()
	}()
	procctx.CloseAfterContext(px, ctx) // parent cancelled first

	return bs
}

// Bitswap instances implement the bitswap protocol.
type Bitswap struct {
	//	pm *bspm.PeerManager

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// blockstore is the local database
	// NB: ensure threadsafety
	blockstore blockstore.Blockstore

	// newBlocks is a channel for newly added blocks to be provided to the
	// network.  blocks pushed down this channel get buffered and fed to the
	// provideKeys channel later on to avoid too much network activity
	//	newBlocks chan cid.Cid
	// provideKeys directly feeds provide workers
	//	provideKeys chan cid.Cid

	process process.Process

	// Counters for various statistics
	counterLk sync.Mutex
	counters  *counters

	// Metrics interface metrics
	dupMetric         metrics.Histogram
	allMetric         metrics.Histogram
	sentHistogram     metrics.Histogram
	sendTimeHistogram metrics.Histogram

	// External statistics interface
	tracer Tracer

	// // the SessionManager routes requests to interested sessions
	// sm *bssm.SessionManager

	// // the SessionInterestManager keeps track of which sessions are interested
	// // in which CIDs
	// sim *bssim.SessionInterestManager

	// whether or not to make provide announcements
	provideEnabled bool

	// how long to wait before looking for providers in a session
	provSearchDelay time.Duration

	// how often to rebroadcast providing requests to find more optimized providers
	rebroadcastDelay delay.D

	// how many worker threads to start for decision engine blockstore worker
	engineBstoreWorkerCount int

	// how many worker threads to start for decision engine task worker
	engineTaskWorkerCount int

	// the total number of simultaneous threads sending outgoing messages
	taskWorkerCount int

	// the total amount of bytes that a peer should have outstanding, it is utilized by the decision engine
	engineMaxOutstandingBytesPerPeer int

	// the score ledger used by the decision engine
	engineScoreLedger deciface.ScoreLedger

	// target message size setting for engines peer task queue
	engineTargetMessageSize int

	// indicates what to do when the engine receives a want-block for a block that
	// is not in the blockstore. Either send DONT_HAVE or do nothing.
	// This is used to simulate older versions of bitswap that did nothing instead of sending back a DONT_HAVE.
	engineSetSendDontHaves bool

	// whether we should actually simulate dont haves on request timeout
	simulateDontHavesOnTimeout bool

	taskComparator TaskComparator

	// an optional feature to accept / deny requests for blocks
	peerBlockRequestFilter PeerBlockRequestFilter

	server Server
	client Client

	provider *provider
}

type counters struct {
	blocksRecvd    uint64
	dupBlocksRecvd uint64
	dupDataRecvd   uint64
	blocksSent     uint64
	dataSent       uint64
	dataRecvd      uint64
	messagesRecvd  uint64
}

// GetBlock attempts to retrieve a particular block from peers within the
// deadline enforced by the context.
func (bs *Bitswap) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "GetBlock", trace.WithAttributes(attribute.String("Key", k.String())))
	defer span.End()
	return bsgetter.SyncGetBlock(ctx, k, bs.GetBlocks)
}

// WantlistForPeer returns the currently understood list of blocks requested by a
// given peer.
func (bs *Bitswap) WantlistForPeer(p peer.ID) []cid.Cid {
	return bs.server.WantlistForPeer(p)
}

// LedgerForPeer returns aggregated data about blocks swapped and communication
// with a given peer.
func (bs *Bitswap) LedgerForPeer(p peer.ID) *decision.Receipt {
	return bs.server.LedgerForPeer(p)
}

// GetBlocks returns a channel where the caller may receive blocks that
// correspond to the provided |keys|. Returns an error if BitSwap is unable to
// begin this request within the deadline enforced by the context.
//
// NB: Your request remains open until the context expires. To conserve
// resources, provide a context with a reasonably short deadline (ie. not one
// that lasts throughout the lifetime of the server)
func (bs *Bitswap) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "GetBlocks", trace.WithAttributes(attribute.Int("NumKeys", len(keys))))
	defer span.End()
	return bs.client.GetBlocks(ctx, keys)
}

// HasBlock announces the existence of a block to this bitswap service. The
// service will potentially notify its peers.
func (bs *Bitswap) HasBlock(ctx context.Context, blk blocks.Block) error {
	ctx, span := internal.StartSpan(ctx, "GetBlocks", trace.WithAttributes(attribute.String("Block", blk.Cid().String())))
	defer span.End()
	return bs.client.ReceivedBlocksFrom(ctx, "", []blocks.Block{blk}, nil, nil)
}

// ReceiveMessage is called by the network interface when a new message is
// received.
func (bs *Bitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) {
	bs.counterLk.Lock()
	bs.counters.messagesRecvd++
	bs.counterLk.Unlock()

	iblocks := incoming.Blocks()
	if len(iblocks) > 0 {
		bs.updateReceiveCounters(iblocks)
		for _, b := range iblocks {
			log.Debugf("[recv] block; cid=%s, peer=%s", b.Cid(), p)
		}
	}

	bs.server.ReceiveMessage(ctx, p, incoming)
	bs.client.ReceiveMessage(ctx, p, incoming)

	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}
}

func (bs *Bitswap) updateReceiveCounters(blocks []blocks.Block) {
	// Check which blocks are in the datastore
	// (Note: any errors from the blockstore are simply logged out in
	// blockstoreHas())
	blocksHas := bs.blockstoreHas(blocks)

	bs.counterLk.Lock()
	defer bs.counterLk.Unlock()

	// Do some accounting for each block
	for i, b := range blocks {
		has := blocksHas[i]

		blkLen := len(b.RawData())
		bs.allMetric.Observe(float64(blkLen))
		if has {
			bs.dupMetric.Observe(float64(blkLen))
		}

		c := bs.counters

		c.blocksRecvd++
		c.dataRecvd += uint64(blkLen)
		if has {
			c.dupBlocksRecvd++
			c.dupDataRecvd += uint64(blkLen)
		}
	}
}

func (bs *Bitswap) blockstoreHas(blks []blocks.Block) []bool {
	res := make([]bool, len(blks))

	wg := sync.WaitGroup{}
	for i, block := range blks {
		wg.Add(1)
		go func(i int, b blocks.Block) {
			defer wg.Done()

			has, err := bs.blockstore.Has(context.TODO(), b.Cid())
			if err != nil {
				log.Infof("blockstore.Has error: %s", err)
				has = false
			}

			res[i] = has
		}(i, block)
	}
	wg.Wait()

	return res
}

// PeerConnected is called by the network interface
// when a peer initiates a new connection to bitswap.
func (bs *Bitswap) PeerConnected(p peer.ID) {
	bs.client.PeerConnected(p)
	bs.server.PeerConnected(p)
}

// PeerDisconnected is called by the network interface when a peer
// closes a connection
func (bs *Bitswap) PeerDisconnected(p peer.ID) {
	bs.client.PeerConnected(p)
	bs.server.PeerDisconnected(p)
}

// Close is called to shutdown Bitswap
func (bs *Bitswap) Close() error {
	return bs.process.Close()
}

// GetWantlist returns the current local wantlist (both want-blocks and
// want-haves).
func (bs *Bitswap) GetWantlist() []cid.Cid {
	return bs.client.GetWantlist()
}

// GetWantBlocks returns the current list of want-blocks.
func (bs *Bitswap) GetWantBlocks() []cid.Cid {
	return bs.client.GetWantBlocks()
}

// GetWanthaves returns the current list of want-haves.
func (bs *Bitswap) GetWantHaves() []cid.Cid {
	return bs.client.GetWantHaves()
}

// IsOnline is needed to match go-ipfs-exchange-interface
func (bs *Bitswap) IsOnline() bool {
	return true
}

// NewSession generates a new Bitswap session. You should use this, rather
// that calling Bitswap.GetBlocks, any time you intend to do several related
// block requests in a row. The session returned will have it's own GetBlocks
// method, but the session will use the fact that the requests are related to
// be more efficient in its requests to peers. If you are using a session
// from go-blockservice, it will create a bitswap session automatically.
func (bs *Bitswap) NewSession(ctx context.Context) exchange.Fetcher {
	ctx, span := internal.StartSpan(ctx, "NewSession")
	defer span.End()
	return bs.client.NewSession(ctx)
}
