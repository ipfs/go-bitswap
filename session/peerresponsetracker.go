package session

import (
	"math/rand"
	"sync"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

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

	rnd := rand.Float64()
	total := 0
	var candidates []peer.ID

	prt.RLock()
	defer prt.RUnlock()

	for _, p := range peers {
		if count, ok := prt.firstResponder[p]; ok {
			total += count + 1
			candidates = append(candidates, p)
		}
	}

	counted := 0.0
	for _, p := range candidates {
		counted += float64(prt.firstResponder[p]+1) / float64(total)
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
