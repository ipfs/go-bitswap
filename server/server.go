// Package server implements the Bitswap protocol as a server
package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"

	deciface "github.com/ipfs/go-bitswap/decision"
	"github.com/ipfs/go-bitswap/internal/decision"
	"github.com/ipfs/go-bitswap/internal/defaults"
	"github.com/ipfs/go-bitswap/internal/notifications"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	bssm "github.com/ipfs/go-bitswap/internal/sessionmanager"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-metrics-interface"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")
var sflog = log.Desugar()

var (
	// the 1<<18+15 is to observe old file chunks that are 1<<18 + 14 in size
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}

	timeMetricsBuckets = []float64{1, 10, 30, 60, 90, 120, 600}
)

// Option defines the functional option type that can be used to configure
// bitswap instances
type Option func(*Bitswap)

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

// New initializes a BitSwap instance that communicates over the provided
// BitSwapNetwork. This function registers the returned instance as the network
// delegate. Runs until context is cancelled or bitswap.Close is called.
func New(parent context.Context, network bsnet.BitSwapNetwork,
	bstore blockstore.Blockstore, options ...Option) *Bitswap {

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

	pendingEngineGauge := metrics.NewCtx(ctx, "pending_tasks", "Total number of pending tasks").Gauge()

	activeEngineGauge := metrics.NewCtx(ctx, "active_tasks", "Total number of active tasks").Gauge()

	pendingBlocksGauge := metrics.NewCtx(ctx, "pending_block_tasks", "Total number of pending blockstore tasks").Gauge()

	activeBlocksGauge := metrics.NewCtx(ctx, "active_block_tasks", "Total number of active blockstore tasks").Gauge()

	px := process.WithTeardown(func() error {
		return nil
	})

	// onDontHaveTimeout is called when a want-block is sent to a peer that
	// has an old version of Bitswap that doesn't support DONT_HAVE messages,
	// or when no response is received within a timeout.
	var bs *Bitswap

	notif := notifications.New()

	bs = &Bitswap{
		blockstore:                       bstore,
		network:                          network,
		process:                          px,
		counters:                         new(counters),
		dupMetric:                        dupHist,
		allMetric:                        allHist,
		sentHistogram:                    sentHistogram,
		sendTimeHistogram:                sendTimeHistogram,
		engineBstoreWorkerCount:          defaults.BitswapEngineBlockstoreWorkerCount,
		engineTaskWorkerCount:            defaults.BitswapEngineTaskWorkerCount,
		taskWorkerCount:                  defaults.BitswapTaskWorkerCount,
		engineMaxOutstandingBytesPerPeer: defaults.BitswapMaxOutstandingBytesPerPeer,
		engineSetSendDontHaves:           true,
	}

	// apply functional options before starting and running bitswap
	for _, option := range options {
		option(bs)
	}

	// Set up decision engine
	bs.engine = decision.NewEngine(
		ctx,
		bstore,
		bs.engineBstoreWorkerCount,
		bs.engineTaskWorkerCount,
		bs.engineMaxOutstandingBytesPerPeer,
		network.ConnectionManager(),
		network.Self(),
		bs.engineScoreLedger,
		pendingEngineGauge,
		activeEngineGauge,
		pendingBlocksGauge,
		activeBlocksGauge,
	)
	bs.engine.SetSendDontHaves(bs.engineSetSendDontHaves)

	network.SetDelegate(bs)

	// Start up bitswaps async worker routines
	bs.startWorkers(ctx, px)
	bs.engine.StartWorkers(ctx, px)

	// bind the context and process.
	// do it over here to avoid closing before all setup is done.
	go func() {
		<-px.Closing() // process closes first
		cancelFunc()
		notif.Shutdown()
	}()
	procctx.CloseAfterContext(px, ctx) // parent cancelled first

	return bs
}

// Bitswap instances implement the bitswap protocol.
type Bitswap struct {
	// the engine is the bit of logic that decides who to send which blocks to
	engine *decision.Engine

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// blockstore is the local database
	// NB: ensure threadsafety
	blockstore blockstore.Blockstore

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
	wiretap WireTap

	// the SessionManager routes requests to interested sessions
	sm *bssm.SessionManager

	// the SessionInterestManager keeps track of which sessions are interested
	// in which CIDs
	sim *bssim.SessionInterestManager

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

	// indicates what to do when the engine receives a want-block for a block that
	// is not in the blockstore. Either send DONT_HAVE or do nothing.
	// This is used to simulate older versions of bitswap that did nothing instead of sending back a DONT_HAVE.
	engineSetSendDontHaves bool

	// whether we should actually simulate dont haves on request timeout
	simulateDontHavesOnTimeout bool
}

func (bs *Bitswap) PeerConnected(id peer.ID) {
	// TODO: This is a silly interface requirement
}

func (bs *Bitswap) PeerDisconnected(id peer.ID) {
	// TODO: This is a silly interface requirement
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

// WantlistForPeer returns the currently understood list of blocks requested by a
// given peer.
func (bs *Bitswap) WantlistForPeer(p peer.ID) []cid.Cid {
	var out []cid.Cid
	for _, e := range bs.engine.WantlistForPeer(p) {
		out = append(out, e.Cid)
	}
	return out
}

// LedgerForPeer returns aggregated data about blocks swapped and communication
// with a given peer.
func (bs *Bitswap) LedgerForPeer(p peer.ID) *decision.Receipt {
	return bs.engine.LedgerForPeer(p)
}

// ReceiveMessage is called by the network interface when a new message is
// received.
func (bs *Bitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) {
	bs.counterLk.Lock()
	bs.counters.messagesRecvd++
	bs.counterLk.Unlock()

	// This call records changes to wantlists, blocks received,
	// and number of bytes transfered.
	bs.engine.MessageReceived(ctx, p, incoming)
	// TODO: this is bad, and could be easily abused.
	// Should only track *useful* messages in ledger

	if bs.wiretap != nil {
		bs.wiretap.MessageReceived(p, incoming)
	}
}

// ReceiveError is called by the network interface when an error happens
// at the network layer. Currently just logs error.
func (bs *Bitswap) ReceiveError(err error) {
	log.Infof("Bitswap ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

// Close is called to shutdown Bitswap
func (bs *Bitswap) Close() error {
	return bs.process.Close()
}
