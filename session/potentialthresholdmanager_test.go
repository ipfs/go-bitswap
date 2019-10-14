package session

import (
	"testing"
)

func TestPotentialThreshold(t *testing.T) {
	ptm := newPotentialThresholdManager()
	if ptm.PotentialThreshold() != initialPotentialThreshold {
		t.Fatal("incorrect initial potential threshold")
	}

	// There should be no change until >= than the minimum number of messages
	// have been received
	total := minPotentialThresholdItemCount
	ptm.Received(total-1, 0)
	if ptm.PotentialThreshold() != initialPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}

	// Once we have enough blocks, the threshold should vary by the ratio of
	// misses to hits
	ptm.Received(1, 0)
	if ptm.PotentialThreshold() != minPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}

	hits := total
	misses := 0
	ratio := 0.0

	// Add misses until the ratio rises above the maximum
	for ratio <= maxPotentialThreshold {
		ptm.Received(0, 1)
		misses++
		ratio = float64(misses) / float64(hits)
	}

	// At this point the threshold should switch to the maximum
	if ptm.PotentialThreshold() != maxPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}

	// Add hits until the ratio drops below the minimum
	for ratio >= minPotentialThreshold {
		ptm.Received(1, 0)
		hits++
		ratio = float64(misses) / float64(hits)
	}

	// At this point the threshold should switch to the minimum
	if ptm.PotentialThreshold() != minPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}
}
