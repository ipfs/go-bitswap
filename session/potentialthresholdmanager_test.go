package session

import (
	"testing"
)

func TestPotentialThreshold(t *testing.T) {
	min := 0.5
	max := 0.8
	missTolerance := 0.05
	capacity := 100
	significanceCount := 3
	ptm := newPotentialThresholdManager(min, max, missTolerance, capacity, significanceCount)
	if ptm.PotentialThreshold() != max {
		t.Fatal("incorrect initial potential threshold")
	}

	// There should be no change until >= than the minimum number of messages
	// have been received
	ptm.Received(significanceCount-1, 0)
	if ptm.PotentialThreshold() != max {
		t.Fatal("incorrect potential threshold")
	}

	// Once we have enough blocks, the threshold should vary by the ratio of
	// misses to hits
	ptm.Received(1, 0)

	// Should be at the minimum value
	if ptm.PotentialThreshold() != min {
		t.Fatal("incorrect potential threshold")
	}

	// Add misses until the value rises to the maximum
	for ptm.PotentialThreshold() < max {
		ptm.Received(0, 1)
	}

	// At this point the threshold should be at the maximum
	if ptm.PotentialThreshold() != max {
		t.Fatal("incorrect potential threshold", ptm.PotentialThreshold())
	}

	// Add hits until the value drops to the minimum
	for ptm.PotentialThreshold() > min {
		ptm.Received(1, 0)
	}

	// At this point the threshold should switch to the minimum
	if ptm.PotentialThreshold() != min {
		t.Fatal("incorrect potential threshold")
	}
}

func TestPotentialThresholdMissTolerance(t *testing.T) {
	min := 0.5
	max := 0.8
	missTolerance := 0.05
	capacity := 100
	significanceCount := 0
	ptm := newPotentialThresholdManager(min, max, missTolerance, capacity, significanceCount)

	ptm.Received(100, 0)
	if ptm.PotentialThreshold() != min {
		t.Fatal("incorrect potential threshold")
	}

	// Only hits received so far, should be at the minimum value
	if ptm.PotentialThreshold() != min {
		t.Fatal("incorrect potential threshold")
	}

	// Until we go over the miss tolerance, should remain at the minimum value
	ptm.Received(0, 5)
	if ptm.PotentialThreshold() != min {
		t.Fatal("incorrect potential threshold")
	}

	// Once we breach the miss tolerance, should now be above the minimum value
	ptm.Received(0, 1)
	if ptm.PotentialThreshold() <= min {
		t.Fatal("incorrect potential threshold")
	}
}

func TestPotentialThresholdExceedCapacity(t *testing.T) {
	min := 0.5
	max := 0.8
	missTolerance := 0.05
	capacity := 10
	significanceCount := 0
	ptm := newPotentialThresholdManager(min, max, missTolerance, capacity, significanceCount)

	ptm.Received(50, 20)
	if ptm.PotentialThreshold() <= min || ptm.PotentialThreshold() >= max {
		t.Fatal("incorrect potential threshold")
	}
}
