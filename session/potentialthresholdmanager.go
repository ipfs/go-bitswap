package session

import (
	"fmt"
	"math"
	"sync"

	cid "github.com/ipfs/go-cid"
)

const (
	minPotentialThreshold     = 0.5
	maxPotentialThreshold     = 0.8
	initialPotentialThreshold = maxPotentialThreshold
	// The number of recent blocks to keep track of uniq / dups for
	maxPotentialThresholdRecent = 256
	// Start calculating potential threshold after we have this many blocks
	minPotentialThresholdBlockCount = 8
	// If the ratio of duplicates / total blocks
	// - is under low watermark: jump to maxPotentialThreshold
	// - is over high watermark: drop to minPotentialThreshold
	dupRatioLowWatermark  = 0.05
	dupRatioHighWatermark = 0.2
)

type PotentialThresholdManager interface {
	PotentialThreshold() float64
	IdleTimeout()
	ReceivedBlocks(uniqs []cid.Cid, dups []cid.Cid)
}

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
	ptm.potentialThreshold = maxPotentialThreshold
}

func (ptm *potentialThresholdManager) ReceivedBlocks(uniqs []cid.Cid, dups []cid.Cid) {
	// If there are too many received blocks to fit into the running average,
	// add them proportionally
	total := float64(len(uniqs) + len(dups))
	if len(uniqs) > 0 && len(dups) > 0 && total > maxPotentialThresholdRecent {
		uniqRatio := int(math.Floor(float64(len(uniqs)) * maxPotentialThresholdRecent / total))
		dupRatio := int(math.Floor(float64(len(dups)) * maxPotentialThresholdRecent / total))
		uniqs = uniqs[:uniqRatio]
		dups = dups[:dupRatio]
	}

	// Record unique vs duplicate blocks
	for range uniqs {
		ptm.recentWasUnique = append(ptm.recentWasUnique, true)
	}
	for range dups {
		ptm.recentWasUnique = append(ptm.recentWasUnique, false)
	}

	poplen := len(ptm.recentWasUnique) - maxPotentialThresholdRecent
	if poplen > 0 {
		ptm.recentWasUnique = ptm.recentWasUnique[poplen:]
	}

	if len(ptm.recentWasUnique) > minPotentialThresholdBlockCount {
		unqCount := 0
		dupCount := 0
		for _, u := range ptm.recentWasUnique {
			if u {
				unqCount++
			} else {
				dupCount++
			}
		}
		total := unqCount + dupCount
		dupTotalRatio := float64(dupCount) / float64(total)
		if dupTotalRatio < dupRatioLowWatermark {
			if ptm.potentialThreshold != maxPotentialThreshold {
				// ptm.log.log("threshold-up: %.2f (dup %d / total %d = %.2f which is < %.2f so ↑ threshold to %.2f",
				// 	ptm.potentialThreshold, dupCount, total, dupTotalRatio, dupRatioLowWatermark, maxPotentialThreshold)
			}
			ptm.potentialThreshold = maxPotentialThreshold
		} else if dupTotalRatio > dupRatioHighWatermark {
			if ptm.potentialThreshold != minPotentialThreshold {
				// ptm.log.log("threshold-dn: %.2f (dup %d / total %d = %.2f which is > %.2f so ↓ threshold to %.2f",
				// 	ptm.potentialThreshold, dupCount, total, dupTotalRatio, dupRatioHighWatermark, minPotentialThreshold)
			}
			ptm.potentialThreshold = minPotentialThreshold
		}
	}
}
