package tracer

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/tracer"
)

// Tracer provides methods to access all messages sent and received by Bitswap.
// This interface can be used to implement various statistics (this is original intent).
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/tracer.Tracer instead
type Tracer = libipfs.Tracer
