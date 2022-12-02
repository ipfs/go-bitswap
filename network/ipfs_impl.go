package network

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/routing"
)

// NewFromIpfsHost returns a BitSwapNetwork supported by underlying IPFS host.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.NewFromIpfsHost instead
func NewFromIpfsHost(host host.Host, r routing.ContentRouting, opts ...NetOpt) BitSwapNetwork {
	return libipfs.NewFromIpfsHost(host, r, opts...)
}
