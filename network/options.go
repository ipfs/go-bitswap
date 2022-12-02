package network

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.NetOpt instead
type NetOpt = libipfs.NetOpt

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.Settings instead
type Settings = libipfs.Settings

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.Prefix instead
func Prefix(prefix protocol.ID) NetOpt {
	return libipfs.Prefix(prefix)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.SupportedProtocols instead
func SupportedProtocols(protos []protocol.ID) NetOpt {
	return libipfs.SupportedProtocols(protos)
}
