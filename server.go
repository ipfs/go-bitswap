package bitswap

import (
	"context"

	"github.com/ipfs/go-bitswap/internal/decision"
	"github.com/ipfs/go-bitswap/internal/defaults"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-metrics-interface"
	process "github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p-core/peer"
)

type providerFunc func(context.Context, []blocks.Block) error

type server struct {
	px       process.Process
	engine   *decision.Engine
	provider providerFunc
}

type serverOpts struct {
	taskComparator              TaskComparator // nil-safe
	targetMessageSize           int
	peerBlockRequestFilter      PeerBlockRequestFilter // nil-safe
	sendDontHaves               bool
	scoreLedger                 decision.ScoreLedger
	engineBlockstoreWorkerCount int
	engineTaskWorkerCount       int
	maxOutstandingBytesPerPeer  int
}

type ServerOption func(*serverOpts)

func WithServerTaskComparator(comparator TaskComparator) ServerOption {
	return func(o *serverOpts) {
		o.taskComparator = comparator
	}
}

func WithServerTargetMessageSize(n int) ServerOption {
	return func(o *serverOpts) {
		o.targetMessageSize = n
	}
}
func WithServerPeerBlockRequestFilter(p PeerBlockRequestFilter) ServerOption {
	return func(o *serverOpts) {
		o.peerBlockRequestFilter = p
	}
}

func WithServerSetSendDontHaves(send bool) ServerOption {
	return func(o *serverOpts) {
		o.sendDontHaves = send
	}
}

func WithServerScoreLedger(s decision.ScoreLedger) ServerOption {
	return func(o *serverOpts) {
		o.scoreLedger = s
	}
}

func WithServerEngineBlockstoreWorkerCount(n int) ServerOption {
	return func(o *serverOpts) {
		o.engineBlockstoreWorkerCount = n
	}
}

func WithServerEngineTaskWorkerCount(n int) ServerOption {
	return func(o *serverOpts) {
		o.engineTaskWorkerCount = n
	}
}
func WithServerMaxOutstandingBytesPerPeer(n int) ServerOption {
	return func(o *serverOpts) {
		o.maxOutstandingBytesPerPeer = n
	}
}

func NewServer(ctx context.Context, px process.Process, bs blockstore.Blockstore, network bsnet.BitSwapNetwork, provider providerFunc, opts ...ServerOption) *server {
	serverOpts := &serverOpts{
		targetMessageSize:           defaults.BitswapEngineTargetMessageSize,
		engineBlockstoreWorkerCount: defaults.BitswapEngineBlockstoreWorkerCount,
		engineTaskWorkerCount:       defaults.BitswapEngineTaskWorkerCount,
		maxOutstandingBytesPerPeer:  defaults.BitswapMaxOutstandingBytesPerPeer,
	}
	for _, o := range opts {
		o(serverOpts)
	}
	if serverOpts.scoreLedger == nil {
		serverOpts.scoreLedger = decision.NewDefaultScoreLedger()
	}

	pendingEngineGauge := metrics.NewCtx(ctx, "pending_tasks", "Total number of pending tasks").Gauge()
	activeEngineGauge := metrics.NewCtx(ctx, "active_tasks", "Total number of active tasks").Gauge()
	pendingBlocksGauge := metrics.NewCtx(ctx, "pending_block_tasks", "Total number of pending blockstore tasks").Gauge()
	activeBlocksGauge := metrics.NewCtx(ctx, "active_block_tasks", "Total number of active blockstore tasks").Gauge()

	engine := decision.NewEngine(
		ctx,
		bs,
		serverOpts.engineBlockstoreWorkerCount,
		serverOpts.engineTaskWorkerCount,
		serverOpts.maxOutstandingBytesPerPeer,
		network.ConnectionManager(),
		network.Self(),
		serverOpts.scoreLedger,
		pendingEngineGauge,
		activeEngineGauge,
		pendingBlocksGauge,
		activeBlocksGauge,
		decision.WithTaskComparator(serverOpts.taskComparator),
		decision.WithTargetMessageSize(serverOpts.targetMessageSize),
		decision.WithPeerBlockRequestFilter(serverOpts.peerBlockRequestFilter),
	)

	engine.SetSendDontHaves(serverOpts.sendDontHaves)

	s := &server{
		px:       px,
		engine:   engine,
		provider: provider,
	}

	s.engine.StartWorkers(ctx, px)

	return s
}

func (s *server) ReceiveMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	s.engine.MessageReceived(ctx, p, m)
}

func (s *server) PeerConnected(p peer.ID) {
	s.engine.PeerConnected(p)
}
func (s *server) PeerDisconnected(p peer.ID) {
	s.engine.PeerDisconnected(p)
}
func (s *server) ReceivedBlocks(ctx context.Context, p peer.ID, blks []blocks.Block) {
	s.engine.ReceiveFrom(p, blks)
	s.provider(ctx, blks)
}

func (s *server) WantlistForPeer(p peer.ID) []cid.Cid {
	var out []cid.Cid
	for _, e := range s.engine.WantlistForPeer(p) {
		out = append(out, e.Cid)
	}
	return out
}

func (s *server) LedgerForPeer(p peer.ID) *decision.Receipt {
	return s.engine.LedgerForPeer(p)
}

func (s *server) Peers() []peer.ID {
	return s.engine.Peers()
}
