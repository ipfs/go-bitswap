package session

import (
	"fmt"
	"math"
	"sync"
)

type PotentialThresholdManager interface {
	PotentialThreshold() float64
	Received(hits int, misses int)
}

// potentialThresholdManager keeps track of the threshold value below which
// wants can be sent to peers for a given CID. Once the sent potential is
// above the threshold, wants can no longer be sent out for the CID.
type potentialThresholdManager struct {
	sync.RWMutex
	// TODO: Use more efficient data structure than bool array
	recentWasHit []bool

	potentialThreshold float64
	// Base threshold value
	base float64
	// Minimum threshold value
	min float64
	// Maximum threshold value
	max float64
	// The number of recent blocks to keep track of hits and misses for
	capacity int
	// Start calculating potential threshold after we have this many blocks
	significanceCount int
}

func newPotentialThresholdManager(min float64, max float64, missTolerance float64, capacity int, significanceCount int) *potentialThresholdManager {
	return &potentialThresholdManager{
		potentialThreshold: max,
		min:                min,
		max:                max,
		base:               (1 - missTolerance) * min,
		capacity:           capacity,
		significanceCount:  significanceCount,
	}
}

func (ptm *potentialThresholdManager) String() string {
	return fmt.Sprintf("Potential threshold: %.2f", ptm.potentialThreshold)
}

func (ptm *potentialThresholdManager) PotentialThreshold() float64 {
	return ptm.potentialThreshold
}

func (ptm *potentialThresholdManager) Received(hits int, misses int) {
	// If there are too many received items to fit into the running average,
	// add them proportionally
	total := float64(hits + misses)
	if hits > 0 && misses > 0 && total > float64(ptm.capacity) {
		hits = int(math.Floor(float64(hits*ptm.capacity) / total))
		misses = int(math.Floor(float64(misses*ptm.capacity) / total))
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
	poplen := len(ptm.recentWasHit) - ptm.capacity
	if poplen > 0 {
		ptm.recentWasHit = ptm.recentWasHit[poplen:]
	}

	// Wait until there are enough received items to be significant
	if len(ptm.recentWasHit) < ptm.significanceCount {
		return
	}

	// Count how many hits / misses there have been in the last
	// ptm.significanceCount recordings
	hitCount := 0
	missCount := 0
	for _, u := range ptm.recentWasHit {
		if u {
			hitCount++
		} else {
			missCount++
		}
	}

	// Number of peers to send each want to is inversely proportional to the
	// hit rate, eg:
	//   xxx Hit rate: 0/3, so num peers should be max
	//   ✓xx Hit rate: 1/3, so num peers should be 3/1
	//   ✓✓x Hit rate: 2/3, so num peers should be 3/2
	//   ✓✓✓ Hit rate: 3/3, so num peers should be 1

	// If we didn't get any hits, peg threshold to the maximum value
	if hitCount == 0 {
		ptm.potentialThreshold = ptm.max
		return
	}

	// Calculate the inverse of the hit rate
	totalCount := hitCount + missCount
	numPeers := float64(totalCount) / float64(hitCount)

	// Adjust proportionally to the base value
	ratio := numPeers * ptm.base

	// Ensure value is between min and max
	if ratio > ptm.max {
		ratio = ptm.max
	} else if ratio < ptm.min {
		ratio = ptm.min
	}

	ptm.potentialThreshold = ratio
}
