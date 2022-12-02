package bitswap

import (
	"context"

	mockrouting "github.com/ipfs/go-ipfs-routing/mock"

	libipfs "github.com/ipfs/go-libipfs/bitswap/testnet"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// StreamNet is a testnet that uses libp2p's MockNet
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.StreamNet instead
func StreamNet(ctx context.Context, net mockpeernet.Mocknet, rs mockrouting.Server) (Network, error) {
	return libipfs.StreamNet(ctx, net, rs)
}
