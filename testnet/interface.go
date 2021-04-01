package bitswap

import (
	"github.com/libp2p/go-libp2p-core/peer"
	tnet "github.com/libp2p/go-libp2p-testing/net"

	bsnet "github.com/daotl/go-bitswap/network"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
type Network interface {
	Adapter(tnet.Identity, ...bsnet.NetOpt) bsnet.BitSwapNetwork

	HasPeer(peer.ID) bool
}
