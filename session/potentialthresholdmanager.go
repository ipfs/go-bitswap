package session

import (
	"fmt"
	"math"
	"sync"
)

const (
	minPotentialThreshold     = 0.5
	maxPotentialThreshold     = 0.8
	initialPotentialThreshold = maxPotentialThreshold
	// The number of recent blocks to keep track of hits and misses for
	maxPotentialThresholdRecent = 256
	// Start calculating potential threshold after we have this many blocks
	minPotentialThresholdItemCount = 8
)

type PotentialThresholdManager interface {
	PotentialThreshold() float64
	IdleTimeout()
	Received(hits int, misses int)
}

type potentialThresholdManager struct {
	sync.RWMutex
	potentialThreshold float64
	recentWasHit       []bool
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

func (ptm *potentialThresholdManager) Received(hits int, misses int) {
	// If there are too many received items to fit into the running average,
	// add them proportionally
	total := float64(hits + misses)
	if hits > 0 && misses > 0 && total > maxPotentialThresholdRecent {
		hits = int(math.Floor(float64(hits) * maxPotentialThresholdRecent / total))
		misses = int(math.Floor(float64(misses) * maxPotentialThresholdRecent / total))
	}

	// Record hits vs misses
	for hits > 0 || misses > 0 {
		if hits > 0 {
			ptm.recentWasHit = append(ptm.recentWasHit, true)
			hits--
		}
		if misses > 0 {
			ptm.recentWasHit = append(ptm.recentWasHit, false)
			misses--
		}
	}

	// Truncate the list to the maximum length
	poplen := len(ptm.recentWasHit) - maxPotentialThresholdRecent
	if poplen > 0 {
		ptm.recentWasHit = ptm.recentWasHit[poplen:]
	}

	// Wait until there are enough received items to be significant
	if len(ptm.recentWasHit) < minPotentialThresholdItemCount {
		return
	}

	// Count how many hits / misses there have been in the last
	// minPotentialThresholdItemCount recordings
	hitCount := 0
	missCount := 0
	for _, u := range ptm.recentWasHit {
		if u {
			hitCount++
		} else {
			missCount++
		}
	}
	ratio := maxPotentialThreshold
	if hitCount > 0 {
		ratio = float64(missCount) / float64(hitCount)
	}

	// Keep the ratio between min and max
	if ratio > maxPotentialThreshold {
		ptm.potentialThreshold = maxPotentialThreshold
	} else if ratio < minPotentialThreshold {
		ptm.potentialThreshold = minPotentialThreshold
	} else {
		ptm.potentialThreshold = ratio
	}
}
