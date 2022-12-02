package wantlist

import (
	"github.com/ipfs/go-cid"
	libipfs "github.com/ipfs/go-libipfs/bitswap/wantlist"
)

type (
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/wantlist.Entry instead
	Entry = libipfs.Entry
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/wantlist.Wantlist instead
	Wantlist = libipfs.Wantlist
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/wantlist.New instead
func New() *Wantlist {
	return libipfs.New()
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/wantlist.New instead
func NewRefEntry(c cid.Cid, p int32) Entry {
	return libipfs.NewRefEntry(c, p)
}
