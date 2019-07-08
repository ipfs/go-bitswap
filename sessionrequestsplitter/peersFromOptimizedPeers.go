package sessionrequestsplitter

import (
	"math/rand"
	"sync"

	bssd "github.com/ipfs/go-bitswap/sessiondata"
	"github.com/libp2p/go-libp2p-core/peer"
)

type skipMap struct {
	skips         [bssd.MaxOptimizedPeers]int
	start         int
	previousIndex int
	currentIndex  int
}

var skipMapPool = sync.Pool{
	New: func() interface{} {
		return new(skipMap)
	},
}

func newSkipMap(length int) *skipMap {
	sm := skipMapPool.Get().(*skipMap)
	sm.Reset(length)
	return sm
}

func returnSkipMap(sm *skipMap) {
	skipMapPool.Put(sm)
}

func (sm *skipMap) Reset(length int) {
	for i := 0; i < length; i++ {
		sm.skips[i] = i + 1
	}
	sm.start = 0
	sm.currentIndex = 0
	sm.previousIndex = -1
}

func (sm *skipMap) BeginTraverse() {
	sm.currentIndex = sm.start
	sm.previousIndex = -1
}

func (sm *skipMap) Advance() {
	sm.previousIndex = sm.currentIndex
	sm.currentIndex = sm.skips[sm.currentIndex]
}

func (sm *skipMap) RemoveCurrent() {
	if sm.currentIndex == sm.start {
		sm.start = sm.skips[sm.currentIndex]
	} else {
		sm.skips[sm.previousIndex] = sm.skips[sm.currentIndex]
	}
}

func (sm *skipMap) CurrentIndex() int {
	return sm.currentIndex
}

func transformOptimization(optimizationRating float64) float64 {
	return optimizationRating * optimizationRating
}

func getOptimizationTotal(optimizedPeers []bssd.OptimizedPeer) float64 {
	optimizationTotal := 0.0
	for _, optimizedPeer := range optimizedPeers {
		optimizationTotal += transformOptimization(optimizedPeer.OptimizationRating)
	}
	return optimizationTotal
}

func peersFromOptimizedPeers(optimizedPeers []bssd.OptimizedPeer) []peer.ID {
	optimizationTotal := getOptimizationTotal(optimizedPeers)
	sm := newSkipMap(len(optimizedPeers))
	peers := make([]peer.ID, 0, len(optimizedPeers))
	for range optimizedPeers {
		randValue := rand.Float64()
		randTarget := randValue * optimizationTotal
		targetSoFar := 0.0
		sm.BeginTraverse()
		for {
			currentPeer := optimizedPeers[sm.CurrentIndex()]
			currentRating := transformOptimization(currentPeer.OptimizationRating)
			targetSoFar += currentRating
			if targetSoFar+0.000001 >= randTarget {
				peers = append(peers, currentPeer.Peer)
				optimizationTotal -= currentRating
				sm.RemoveCurrent()
				break
			}
			sm.Advance()
		}
	}
	returnSkipMap(sm)
	return peers
}
