package bitswap

import (
	"time"

	"github.com/ipfs/go-bitswap/server"
	"github.com/ipfs/go-bitswap/tracer"
	delay "github.com/ipfs/go-ipfs-delay"
	libipfs "github.com/ipfs/go-libipfs/bitswap"
)

// Option is interface{} of server.Option or client.Option or func(*Bitswap)
// wrapped in a struct to gain strong type checking.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap.Option instead
type Option = libipfs.Option

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.EngineBlockstoreWorkerCount instead
func EngineBlockstoreWorkerCount(count int) Option {
	return libipfs.EngineBlockstoreWorkerCount(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.EngineTaskWorkerCount instead
func EngineTaskWorkerCount(count int) Option {
	return libipfs.EngineTaskWorkerCount(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.MaxOutstandingBytesPerPeer instead
func MaxOutstandingBytesPerPeer(count int) Option {
	return libipfs.MaxOutstandingBytesPerPeer(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.TaskWorkerCount instead
func TaskWorkerCount(count int) Option {
	return libipfs.TaskWorkerCount(count)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.ProvideEnabled instead
func ProvideEnabled(enabled bool) Option {
	return libipfs.ProvideEnabled(enabled)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.SetSendDontHaves instead
func SetSendDontHaves(send bool) Option {
	return libipfs.SetSendDontHaves(send)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.WithPeerBlockRequestFilter instead
func WithPeerBlockRequestFilter(pbrf libipfs.PeerBlockRequestFilter) Option {
	return libipfs.WithPeerBlockRequestFilter(pbrf)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.WithScoreLedger instead
func WithScoreLedger(scoreLedger server.ScoreLedger) Option {
	return libipfs.WithScoreLedger(scoreLedger)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.WithTargetMessageSize instead
func WithTargetMessageSize(tms int) Option {
	return libipfs.WithTargetMessageSize(tms)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.WithTaskComparator instead
func WithTaskComparator(comparator libipfs.TaskComparator) Option {
	return libipfs.WithTaskComparator(comparator)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.ProviderSearchDelay instead
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return libipfs.ProviderSearchDelay(newProvSearchDelay)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.RebroadcastDelay instead
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return libipfs.RebroadcastDelay(newRebroadcastDelay)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.SetSimulateDontHavesOnTimeout instead
func SetSimulateDontHavesOnTimeout(send bool) Option {
	return libipfs.SetSimulateDontHavesOnTimeout(send)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.WithTracer instead
func WithTracer(tap tracer.Tracer) Option {
	return libipfs.WithTracer(tap)
}
