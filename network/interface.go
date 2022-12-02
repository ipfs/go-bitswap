package network

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/network"
)

var (
	// ProtocolBitswapNoVers is equivalent to the legacy bitswap protocol
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.BitSwapNetwork instead
	ProtocolBitswapNoVers = libipfs.ProtocolBitswapNoVers
	// ProtocolBitswapOneZero is the prefix for the legacy bitswap protocol
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.ProtocolBitswapOneZero instead
	ProtocolBitswapOneZero = libipfs.ProtocolBitswapOneZero
	// ProtocolBitswapOneOne is the the prefix for version 1.1.0
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.ProtocolBitswapOneOne instead
	ProtocolBitswapOneOne = libipfs.ProtocolBitswapOneOne
	// ProtocolBitswap is the current version of the bitswap protocol: 1.2.0
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.ProtocolBitswap instead
	ProtocolBitswap = libipfs.ProtocolBitswap
)

// BitSwapNetwork provides network connectivity for BitSwap sessions.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.BitSwapNetwork instead
type BitSwapNetwork = libipfs.BitSwapNetwork

// MessageSender is an interface for sending a series of messages over the bitswap
// network
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.MessageSender instead
type MessageSender = libipfs.MessageSender

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.MessageSenderOpts instead
type MessageSenderOpts = libipfs.MessageSenderOpts

// Receiver is an interface that can receive messages from the BitSwapNetwork.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.Receiver instead
type Receiver = libipfs.Receiver

// Routing is an interface to providing and finding providers on a bitswap
// network.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.Routing instead
type Routing = libipfs.Routing

// Pinger is an interface to ping a peer and get the average latency of all pings
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.Pinger instead
type Pinger = libipfs.Pinger

// Stats is a container for statistics about the bitswap network
// the numbers inside are specific to bitswap, and not any other protocols
// using the same underlying network.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/network.Stats instead
type Stats = libipfs.Stats
