// Package wantlist implements an object for bitswap that contains the keys
// that a given peer wants.
package wantlist

import (
	"sort"

	cid "github.com/ipfs/go-cid"
)

// TODO: Move WantTypeT to its own package
type WantTypeT bool

const (
	WantType_Block = WantTypeT(false)
	WantType_Have  = WantTypeT(true)
)

// Wantlist is a raw list of wanted blocks and their priorities
type Wantlist struct {
	set map[cid.Cid]Entry
}

// Entry is an entry in a want list, consisting of a cid and its priority
type Entry struct {
	Cid      cid.Cid
	Priority int
}

// NewRefEntry creates a new reference tracked wantlist entry.
func NewRefEntry(c cid.Cid, p int) Entry {
	return Entry{
		Cid:      c,
		Priority: p,
	}
}

type entrySlice []Entry

func (es entrySlice) Len() int           { return len(es) }
func (es entrySlice) Swap(i, j int)      { es[i], es[j] = es[j], es[i] }
func (es entrySlice) Less(i, j int) bool { return es[i].Priority > es[j].Priority }

// New generates a new raw Wantlist
func New() *Wantlist {
	return &Wantlist{
		set: make(map[cid.Cid]Entry),
	}
}

// Len returns the number of entries in a wantlist.
func (w *Wantlist) Len() int {
	return len(w.set)
}

// Add adds an entry in a wantlist from CID & Priority, if not already present.
func (w *Wantlist) Add(c cid.Cid, priority int) bool {
	if _, ok := w.set[c]; ok {
		return false
	}

	w.set[c] = Entry{
		Cid:      c,
		Priority: priority,
	}

	return true
}

// AddEntry adds an entry to a wantlist if not already present.
func (w *Wantlist) AddEntry(e Entry) bool {
	if _, ok := w.set[e.Cid]; ok {
		return false
	}
	w.set[e.Cid] = e
	return true
}

// Remove removes the given cid from the wantlist.
func (w *Wantlist) Remove(c cid.Cid) bool {
	_, ok := w.set[c]
	if !ok {
		return false
	}

	delete(w.set, c)
	return true
}

// Contains returns the entry, if present, for the given CID, plus whether it
// was present.
func (w *Wantlist) Contains(c cid.Cid) (Entry, bool) {
	e, ok := w.set[c]
	return e, ok
}

// Entries returns all wantlist entries for a want list.
func (w *Wantlist) Entries() []Entry {
	es := make([]Entry, 0, len(w.set))
	for _, e := range w.set {
		es = append(es, e)
	}
	return es
}

// SortedEntries returns wantlist entries ordered by priority.
func (w *Wantlist) SortedEntries() []Entry {
	es := w.Entries()
	sort.Sort(entrySlice(es))
	return es
}
