// Package wantlist implements an object for bitswap that contains the keys
// that a given peer wants.
package wantlist

import (
	cid "github.com/ipfs/go-cid"
	libipfs "github.com/ipfs/go-libipfs/bitswap/client/wantlist"
)

// Wantlist is a raw list of wanted blocks and their priorities
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client/wantlist.Wantlist instead
type Wantlist = libipfs.Wantlist

// Entry is an entry in a want list, consisting of a cid and its priority
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client/wantlist.Entry instead
type Entry = libipfs.Entry

// NewRefEntry creates a new reference tracked wantlist entry.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client/wantlist.NewRefEntry instead
func NewRefEntry(c cid.Cid, p int32) Entry {
	return libipfs.NewRefEntry(c, p)
}

// New generates a new raw Wantlist
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/client/wantlist.New instead
func New() *Wantlist {
	return libipfs.New()
}
