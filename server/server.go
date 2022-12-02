package server

import (
	"context"

	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/tracer"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	libipfs "github.com/ipfs/go-libipfs/bitswap/server"
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.Option instead
type Option = libipfs.Option

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.Server instead
type Server = libipfs.Server

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.New instead
func New(ctx context.Context, network bsnet.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Server {
	return libipfs.New(ctx, network, bstore, options...)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.TaskWorkerCount instead
func TaskWorkerCount(count int) Option {
	return libipfs.TaskWorkerCount(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.WithTracer instead
func WithTracer(tap tracer.Tracer) Option {
	return libipfs.WithTracer(tap)
}

// ProvideEnabled is an option for enabling/disabling provide announcements
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.ProvideEnabled instead
func ProvideEnabled(enabled bool) Option {
	return libipfs.ProvideEnabled(enabled)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.WithPeerBlockRequestFilter instead
func WithPeerBlockRequestFilter(pbrf PeerBlockRequestFilter) Option {
	return libipfs.WithPeerBlockRequestFilter(pbrf)
}

// WithTaskComparator configures custom task prioritization logic.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.WithTaskComparator instead
func WithTaskComparator(comparator TaskComparator) Option {
	return libipfs.WithTaskComparator(comparator)
}

// Configures the engine to use the given score decision logic.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.WithScoreLedger instead
func WithScoreLedger(scoreLedger ScoreLedger) Option {
	return libipfs.WithScoreLedger(scoreLedger)
}

// EngineTaskWorkerCount sets the number of worker threads used inside the engine
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.EngineTaskWorkerCount instead
func EngineTaskWorkerCount(count int) Option {
	return libipfs.EngineTaskWorkerCount(count)
}

// SetSendDontHaves indicates what to do when the engine receives a want-block
// for a block that is not in the blockstore. Either
// - Send a DONT_HAVE message
// - Simply don't respond
// This option is only used for testing.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.SetSendDontHaves instead
func SetSendDontHaves(send bool) Option {
	return libipfs.SetSendDontHaves(send)
}

// EngineBlockstoreWorkerCount sets the number of worker threads used for
// blockstore operations in the decision engine
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.EngineBlockstoreWorkerCount instead
func EngineBlockstoreWorkerCount(count int) Option {
	return libipfs.EngineBlockstoreWorkerCount(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.WithTargetMessageSize instead
func WithTargetMessageSize(tms int) Option {
	return libipfs.WithTargetMessageSize(tms)
}

// MaxOutstandingBytesPerPeer describes approximately how much work we are will to have outstanding to a peer at any
// given time. Setting it to 0 will disable any limiting.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.MaxOutstandingBytesPerPeer instead
func MaxOutstandingBytesPerPeer(count int) Option {
	return libipfs.MaxOutstandingBytesPerPeer(count)
}

// HasBlockBufferSize configure how big the new blocks buffer should be.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.HasBlockBufferSize instead
func HasBlockBufferSize(count int) Option {
	return libipfs.HasBlockBufferSize(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.Stat instead
type Stat = libipfs.Stat
