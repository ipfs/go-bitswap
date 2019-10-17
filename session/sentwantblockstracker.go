package session

import (
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// sentWantBlocksTracker keeps track of which peers we've sent a  want-block to
type sentWantBlocksTracker struct {
	sentWantBlocks map[peer.ID]*cid.Set
}

func newSentWantBlocksTracker() *sentWantBlocksTracker {
	return &sentWantBlocksTracker{
		sentWantBlocks: make(map[peer.ID]*cid.Set),
	}
}

func (s *sentWantBlocksTracker) addSentWantBlocksTo(p peer.ID, ks []cid.Cid) {
	if _, ok := s.sentWantBlocks[p]; !ok {
		s.sentWantBlocks[p] = cid.NewSet()
	}
	for _, c := range ks {
		s.sentWantBlocks[p].Add(c)
	}
}

func (s *sentWantBlocksTracker) haveSentWantBlockTo(p peer.ID, c cid.Cid) bool {
	if ks, ok := s.sentWantBlocks[p]; ok {
		return ks.Has(c)
	}
	return false
}
