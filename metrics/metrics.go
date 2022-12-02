package metrics

import (
	"context"

	libipfs "github.com/ipfs/go-libipfs/bitswap/metrics"
	"github.com/ipfs/go-metrics-interface"
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.DupHist instead
func DupHist(ctx context.Context) metrics.Histogram {
	return libipfs.DupHist(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.AllHist instead
func AllHist(ctx context.Context) metrics.Histogram {
	return libipfs.AllHist(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.SentHist instead
func SentHist(ctx context.Context) metrics.Histogram {
	return libipfs.SentHist(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.SendTimeHist instead
func SendTimeHist(ctx context.Context) metrics.Histogram {
	return libipfs.SendTimeHist(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.PendingEngineGauge instead
func PendingEngineGauge(ctx context.Context) metrics.Gauge {
	return libipfs.PendingEngineGauge(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.ActiveEngineGauge instead
func ActiveEngineGauge(ctx context.Context) metrics.Gauge {
	return libipfs.ActiveEngineGauge(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.PendingBlocksGauge instead
func PendingBlocksGauge(ctx context.Context) metrics.Gauge {
	return libipfs.PendingBlocksGauge(ctx)
}

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/metrics.ActiveBlocksGauge instead
func ActiveBlocksGauge(ctx context.Context) metrics.Gauge {
	return libipfs.ActiveBlocksGauge(ctx)
}
