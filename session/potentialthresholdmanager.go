package session

import (
	"fmt"
	// "math/rand"
	// "sort"
	"sync"
	// "time"

	// bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"

	cid "github.com/ipfs/go-cid"
	// peer "github.com/libp2p/go-libp2p-core/peer"
)

const (
	initialPotentialThreshold = 1.5
	minPotentialThreshold     = 0.5
	// The number of recent blocks to keep track of uniq / dups for
	maxRecent = 256
	// Start calculating potential threshold after we have this many blocks
	minBlockCount = 16
)

type potentialThresholdManager struct {
	sync.RWMutex
	potentialThreshold float64
	recentWasUnique    []bool
}

func newPotentialThresholdManager() *potentialThresholdManager {
	return &potentialThresholdManager{
		potentialThreshold: initialPotentialThreshold,
	}
}

func (ptm *potentialThresholdManager) String() string {
	return fmt.Sprintf("Potential threshold: %.2f", ptm.potentialThreshold)
}

func (ptm *potentialThresholdManager) PotentialThreshold() float64 {
	return ptm.potentialThreshold
}

func (ptm *potentialThresholdManager) IdleTimeout() {
	ptm.potentialThreshold++
}

func (ptm *potentialThresholdManager) ReceivedBlocks(uniqs []cid.Cid, dups []cid.Cid) {
	// TODO: If we sent a want-have but received a block, (because the block
	// was small enough for the responder to send the block instead of a HAVE)
	// don't count the block as a duplicate.

	// TODO: Handle case where we receive a lot of uniques and a lot of dups
	// (ie more than maxRecent)
	// Record unique vs duplicate blocks
	for range uniqs {
		ptm.recentWasUnique = append(ptm.recentWasUnique, true)
	}
	for range dups {
		ptm.recentWasUnique = append(ptm.recentWasUnique, false)
	}

	poplen := len(ptm.recentWasUnique) - maxRecent
	if poplen > 0 {
		ptm.recentWasUnique = ptm.recentWasUnique[poplen:]
	}

	if len(ptm.recentWasUnique) > minBlockCount {
		unqCount := 1
		dupCount := 1
		for _, u := range ptm.recentWasUnique {
			if u {
				unqCount++
			} else {
				dupCount++
			}
		}
		total := unqCount + dupCount
		uniqTotalRatio := float64(unqCount) / float64(total)
		// log.Debugf("uniq / total: %d / %d = %f\n", unqCount, total, uniqTotalRatio)
		if uniqTotalRatio < 0.8 {
			ptm.decreasePotentialThreshold()
		}
	}
}

func (ptm *potentialThresholdManager) decreasePotentialThreshold() {
	ptm.potentialThreshold = ptm.potentialThreshold * 0.9
	if ptm.potentialThreshold < minPotentialThreshold {
		ptm.potentialThreshold = minPotentialThreshold
	}
	// log.Warningf("potentialThreshold: %f\n", ptm.potentialThreshold)
}
