package bitswap

import (
	"math/rand"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	libipfs "github.com/ipfs/go-libipfs/bitswap/testnet"
)

// InternetLatencyDelayGenerator generates three clusters of delays,
// typical of the type of peers you would encounter on the interenet.
// Given a base delay time T, the wait time generated will be either:
// 1. A normalized distribution around the base time
// 2. A normalized distribution around the base time plus a "medium" delay
// 3. A normalized distribution around the base time plus a "large" delay
// The size of the medium & large delays are determined when the generator
// is constructed, as well as the relative percentages with which delays fall
// into each of the three different clusters, and the standard deviation for
// the normalized distribution.
// This can be used to generate a number of scenarios typical of latency
// distribution among peers on the internet.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.InternetLatencyDelayGenerator instead
func InternetLatencyDelayGenerator(
	mediumDelay time.Duration,
	largeDelay time.Duration,
	percentMedium float64,
	percentLarge float64,
	std time.Duration,
	rng *rand.Rand) delay.Generator {
	return libipfs.InternetLatencyDelayGenerator(mediumDelay, largeDelay, percentMedium, percentLarge, std, rng)
}
