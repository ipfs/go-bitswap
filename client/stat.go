package client

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/client"
)

// Stat is a struct that provides various statistics on bitswap operations
// Deprecated: use github.com/ipfs/go-libipfs/client.Stat instead
type Stat = libipfs.Stat
