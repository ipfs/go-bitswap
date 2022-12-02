package testsession

import (
	"context"

	tn "github.com/ipfs/go-bitswap/testnet"
	libipfsbs "github.com/ipfs/go-libipfs/bitswap"
	libipfsnet "github.com/ipfs/go-libipfs/bitswap/network"
	libipfs "github.com/ipfs/go-libipfs/bitswap/testinstance"
	tnet "github.com/libp2p/go-libp2p-testing/net"
)

// NewTestInstanceGenerator generates a new InstanceGenerator for the given
// testnet
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testinstance.NewTestInstanceGenerator instead
func NewTestInstanceGenerator(net tn.Network, netOptions []libipfsnet.NetOpt, bsOptions []libipfsbs.Option) InstanceGenerator {
	return libipfs.NewTestInstanceGenerator(net, netOptions, bsOptions)
}

// InstanceGenerator generates new test instances of bitswap+dependencies
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testinstance.InstanceGenerator instead
type InstanceGenerator = libipfs.InstanceGenerator

// ConnectInstances connects the given instances to each other
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testinstance.ConnectInstances instead
func ConnectInstances(instances []Instance) {
	libipfs.ConnectInstances(instances)
}

// Instance is a test instance of bitswap + dependencies for integration testing
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testinstance.Instance instead
type Instance = libipfs.Instance

// NewInstance creates a test bitswap instance.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// instances. To safeguard, use the InstanceGenerator to generate instances. It's
// just a much better idea.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testinstance.NewInstance instead
func NewInstance(ctx context.Context, net tn.Network, p tnet.Identity, netOptions []libipfsnet.NetOpt, bsOptions []libipfsbs.Option) Instance {
	return libipfs.NewInstance(ctx, net, p, netOptions, bsOptions)
}
