package bitswap

import (
	"context"

	"github.com/ipfs/go-bitswap/network"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	libipfs "github.com/ipfs/go-libipfs/bitswap"
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.HasBlockBufferSize instead
var HasBlockBufferSize = libipfs.HasBlockBufferSize

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.Bitswap instead
type Bitswap = libipfs.Bitswap

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.New instead
func New(ctx context.Context, net network.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Bitswap {
	return libipfs.New(ctx, net, bstore, options...)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap.Stat instead
type Stat = libipfs.Stat
