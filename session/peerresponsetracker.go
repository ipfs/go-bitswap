package session

import (
	"math/rand"
	"sync"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

// peerResponseTracker keeps track of how many times each peer was the first
// to send us a block for a given CID (used to rank peers)
type peerResponseTracker struct {
	sync.RWMutex
	firstResponder map[peer.ID]int
}

func newPeerResponseTracker() *peerResponseTracker {
	return &peerResponseTracker{
		firstResponder: make(map[peer.ID]int),
	}
}

func (prt *peerResponseTracker) receivedBlockFrom(from peer.ID) {
	prt.Lock()
	defer prt.Unlock()

	count, ok := prt.firstResponder[from]
	if !ok {
		count = 0
	}
	prt.firstResponder[from] = count + 1
}

func (prt *peerResponseTracker) choose(peers []peer.ID) peer.ID {
	if len(peers) == 0 {
		return ""
	}

	// rand makes a syscall so get it outside the lock
	rnd := rand.Float64()

	prt.RLock()
	defer prt.RUnlock()

	// Find the total received blocks for all candidate peers
	total := 0
	for _, p := range peers {
		total += prt.getPeerCount(p)
	}

	// Choose one of the peers with a chance proportional to the number
	// of blocks received from that peer
	counted := 0.0
	for _, p := range peers {
		counted += float64(prt.getPeerCount(p)) / float64(total)
		if counted > rnd {
			// log.Warningf("  chose %s from %s (%d) / %s (%d) with pivot %.2f",
			// 	lu.P(p), lu.P(peers[0]), prt.firstResponder[peers[0]], lu.P(peers[1]), prt.firstResponder[peers[1]], rnd)
			return p
		}
	}

	index := int(rnd * float64(len(peers)))
	// log.Warningf("  chose random (indx %d) %s from %s (%d) / %s (%d) with pivot %.2f",
	// 	index, lu.P(peers[index]), lu.P(peers[0]), prt.firstResponder[peers[0]], lu.P(peers[1]), prt.firstResponder[peers[1]], rnd)
	return peers[index]
}

func (prt *peerResponseTracker) getPeerCount(p peer.ID) int {
	count, ok := prt.firstResponder[p]
	if ok {
		return count
	}

	// Make sure there is always at least a small chance a new peer
	// will be chosen
	return 1
}
