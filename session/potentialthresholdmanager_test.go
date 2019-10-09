package session

import (
	"testing"

	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"
)

func TestPotentialThreshold(t *testing.T) {
	cids := testutil.GenerateCids(maxPotentialThresholdRecent)

	ptm := newPotentialThresholdManager()
	if ptm.PotentialThreshold() != initialPotentialThreshold {
		t.Fatal("incorrect initial potential threshold")
	}

	// There should be no change until more than minBlockCount blocks have been received
	total := minPotentialThresholdBlockCount
	ptm.ReceivedBlocks(cids[:total], []cid.Cid{})
	if ptm.PotentialThreshold() != initialPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}

	// Once we have enough blocks, the threshold should vary by the ratio of
	// duplicate to overall blocks (unique + duplicate)
	dups := 0
	ratio := 0.0

	// Add dups until the dup ratio is above the high watermark
	for ratio <= dupRatioHighWatermark {
		ptm.ReceivedBlocks([]cid.Cid{}, []cid.Cid{cids[total]})
		total++
		dups++
		ratio = float64(dups) / float64(total)
	}

	// At this point the threshold should switch to the minimum
	if ptm.PotentialThreshold() != minPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}

	// Add uniques until the dup ratio is below the low watermark
	for ratio >= dupRatioLowWatermark {
		ptm.ReceivedBlocks([]cid.Cid{cids[total]}, []cid.Cid{})
		total++
		ratio = float64(dups) / float64(total)
	}

	// At this point the threshold should switch to the maximum
	if ptm.PotentialThreshold() != maxPotentialThreshold {
		t.Fatal("incorrect potential threshold")
	}
}
