package sessionrequestsplitter

import (
	"fmt"
	"math"
	"testing"

	bssd "github.com/ipfs/go-bitswap/sessiondata"
	"github.com/ipfs/go-bitswap/testutil"
	"github.com/libp2p/go-libp2p-core/peer"
)

func sampleDistributions(optimizedPeers []bssd.OptimizedPeer, sampleSize int) []map[peer.ID]float64 {
	sampleCounts := make([]map[peer.ID]int, len(optimizedPeers))
	for i := range optimizedPeers {
		sampleCounts[i] = make(map[peer.ID]int)
	}
	for i := 0; i < sampleSize; i++ {
		samplePeers := peersFromOptimizedPeers(optimizedPeers)
		for j, peer := range samplePeers {
			sampleCounts[j][peer] = sampleCounts[j][peer] + 1
		}
	}
	sampleMean := make([]map[peer.ID]float64, len(optimizedPeers))
	for i, sampleCount := range sampleCounts {
		sampleMean[i] = make(map[peer.ID]float64)
		for p, count := range sampleCount {
			sampleMean[i][p] = float64(count) / float64(sampleSize)
		}
	}
	return sampleMean
}

func TestDistributionsFromOptimizations(t *testing.T) {
	peers := testutil.GeneratePeers(3)
	optimizedPeers := make([]bssd.OptimizedPeer, 0, len(peers))
	for i, peer := range peers {
		optimizedPeers = append(optimizedPeers, bssd.OptimizedPeer{peer, 1.0 - (float64(i) * 0.4)})
	}
	distributions := sampleDistributions(optimizedPeers, 1000)
	expectedDistributions := []map[peer.ID]float64{
		map[peer.ID]float64{
			peers[0]: 1.0 / 1.4,
			peers[1]: 0.36 / 1.4,
			peers[2]: 0.04 / 1.4,
		},
		map[peer.ID]float64{
			peers[0]: (0.36/1.4)*(1.0/1.04) + (0.04/1.4)*(1.0/1.36),
			peers[1]: (1.0/1.4)*(0.36/0.4) + (0.04/1.4)*(0.36/1.36),
			peers[2]: (1.0/1.4)*(0.04/0.4) + (0.36/1.4)*(0.04/1.04),
		},
		map[peer.ID]float64{
			peers[0]: (0.04/1.4)*(0.36/1.36) + (0.36/1.4)*(0.04/1.04),
			peers[1]: (0.04/1.4)*(1.0/1.36) + (1.0/1.4)*(0.04/0.4),
			peers[2]: (0.36/1.4)*(1.0/1.04) + (1.0/1.4)*(0.36/0.4),
		},
	}
	for i, distribution := range distributions {
		expectedDistribution := expectedDistributions[i]
		for p, value := range distribution {
			expectedValue := expectedDistribution[p]
			fmt.Printf("Value: %f, Expected: %f\n", value, expectedValue)
			if math.Abs(value-expectedValue) >= 0.02 {
				t.Fatal("Distribution did not match expected distribution")
			}
		}
	}
}
