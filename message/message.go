package message

import (
	"io"

	cid "github.com/ipfs/go-cid"
	msgio "github.com/libp2p/go-msgio"

	libipfs "github.com/ipfs/go-libipfs/bitswap/message"
)

// BitSwapMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the BitSwap protocol.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.BitSwapMessage instead
type BitSwapMessage = libipfs.BitSwapMessage

// Exportable is an interface for structures than can be
// encoded in a bitswap protobuf.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.Exportable instead
type Exportable = libipfs.Exportable

// BlockPresence represents a HAVE / DONT_HAVE for a given Cid
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.BlockPresence instead
type BlockPresence = libipfs.BlockPresence

// Entry is a wantlist entry in a Bitswap message, with flags indicating
// - whether message is a cancel
// - whether requester wants a DONT_HAVE message
// - whether requester wants a HAVE message (instead of the block)
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.Entry instead
type Entry = libipfs.Entry

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.MaxEntrySize instead
var MaxEntrySize = libipfs.MaxEntrySize

// New returns a new, empty bitswap message
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.New instead
func New(full bool) BitSwapMessage {
	return libipfs.New(full)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.BlockPresenceSize instead
func BlockPresenceSize(c cid.Cid) int {
	return libipfs.BlockPresenceSize(c)
}

// FromNet generates a new BitswapMessage from incoming data on an io.Reader.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.FromNet instead
func FromNet(r io.Reader) (BitSwapMessage, error) {
	return libipfs.FromNet(r)
}

// FromPBReader generates a new Bitswap message from a gogo-protobuf reader
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message.FromMsgReader instead
func FromMsgReader(r msgio.Reader) (BitSwapMessage, error) {
	return libipfs.FromMsgReader(r)
}
