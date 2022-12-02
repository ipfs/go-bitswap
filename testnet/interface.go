package bitswap

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/testnet"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.Network instead
type Network = libipfs.Network
