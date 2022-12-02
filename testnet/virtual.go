package bitswap

import (
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"

	libipfs "github.com/ipfs/go-libipfs/bitswap/testnet"
)

// VirtualNetwork generates a new testnet instance - a fake network that
// is used to simulate sending messages.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.VirtualNetwork instead
func VirtualNetwork(rs mockrouting.Server, d delay.D) Network {
	return libipfs.VirtualNetwork(rs, d)
}

// RateLimitGenerator is an interface for generating rate limits across peers
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.RateLimitGenerator instead
type RateLimitGenerator = libipfs.RateLimitGenerator

// RateLimitedVirtualNetwork generates a testnet instance where nodes are rate
// limited in the upload/download speed.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.RateLimitedVirtualNetwork instead
func RateLimitedVirtualNetwork(rs mockrouting.Server, d delay.D, rateLimitGenerator RateLimitGenerator) Network {
	return libipfs.RateLimitedVirtualNetwork(rs, d, rateLimitGenerator)
}
