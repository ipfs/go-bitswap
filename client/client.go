// Package bitswap implements the IPFS exchange interface with the BitSwap
// bilateral exchange protocol.
package client

import (
	"context"

	"time"

	delay "github.com/ipfs/go-ipfs-delay"

	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/tracer"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	libipfs "github.com/ipfs/go-libipfs/bitswap/client"
)

// Option defines the functional option type that can be used to configure
// bitswap instances
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.Option instead
type Option = libipfs.Option

// ProviderSearchDelay overwrites the global provider search delay
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.ProviderSearchDelay instead
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return libipfs.ProviderSearchDelay(newProvSearchDelay)
}

// RebroadcastDelay overwrites the global provider rebroadcast delay
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.RebroadcastDelay instead
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return libipfs.RebroadcastDelay(newRebroadcastDelay)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.SetSimulateDontHavesOnTimeout instead
func SetSimulateDontHavesOnTimeout(send bool) Option {
	return libipfs.SetSimulateDontHavesOnTimeout(send)
}

// Configures the Client to use given tracer.
// This provides methods to access all messages sent and received by the Client.
// This interface can be used to implement various statistics (this is original intent).
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.WithTracer instead
func WithTracer(tap tracer.Tracer) Option {
	return libipfs.WithTracer(tap)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.WithBlockReceivedNotifier instead
func WithBlockReceivedNotifier(brn BlockReceivedNotifier) Option {
	return libipfs.WithBlockReceivedNotifier(brn)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.BlockReceivedNotifier instead
type BlockReceivedNotifier = libipfs.BlockReceivedNotifier

// New initializes a Bitswap client that runs until client.Close is called.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.New instead
func New(parent context.Context, network bsnet.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Client {
	return libipfs.New(parent, network, bstore, options...)
}

// Client instances implement the bitswap protocol.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client.Client instead
type Client = libipfs.Client
