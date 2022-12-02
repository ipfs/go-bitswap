package bitswap

import (
	"math/rand"

	libipfs "github.com/ipfs/go-libipfs/bitswap/testnet"
)

// FixedRateLimitGenerator returns a rate limit generatoe that always generates
// the specified rate limit in bytes/sec.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.FixedRateLimitGenerator instead
func FixedRateLimitGenerator(rateLimit float64) RateLimitGenerator {
	return libipfs.FixedRateLimitGenerator(rateLimit)
}

// VariableRateLimitGenerator makes rate limites that following a normal distribution.
// Deprecated: use github.com/ipfs/go-libipfs/bitswap/testnet.VariableRateLimitGenerator instead
func VariableRateLimitGenerator(rateLimit float64, std float64, rng *rand.Rand) RateLimitGenerator {
	return libipfs.VariableRateLimitGenerator(rateLimit, std, rng)
}
