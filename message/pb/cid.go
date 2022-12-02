package bitswap_message_pb

import (
	libipfs "github.com/ipfs/go-libipfs/bitswap/message/pb"
)

// NOTE: Don't "embed" the cid, wrap it like we're doing here. Otherwise, gogo
// will try to use the Bytes() function.

// Cid is a custom type for CIDs in protobufs, that allows us to avoid
// reallocating.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Cid instead
type Cid = libipfs.Cid
