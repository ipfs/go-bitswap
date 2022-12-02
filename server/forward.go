package server

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/server"
)

type (
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.Receipt instead
	Receipt = libipfs.Receipt
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.PeerBlockRequestFilter instead
	PeerBlockRequestFilter = libipfs.PeerBlockRequestFilter
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.TaskComparator instead
	TaskComparator = libipfs.TaskComparator
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.TaskInfo instead
	TaskInfo = libipfs.TaskInfo
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.ScoreLedger instead
	ScoreLedger = libipfs.ScoreLedger
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/server.ScorePeerFunc instead
	ScorePeerFunc = libipfs.ScorePeerFunc
)
