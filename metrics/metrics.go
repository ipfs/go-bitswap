package metrics

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	defaultBytesDistribution        = view.Distribution(1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296)
	defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
	defaultMessageCountDistribution = view.Distribution(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
)

// Keys
var (
	KeyPeerID, _  = tag.NewKey("peer_id")
	KeyMessage, _ = tag.NewKey("message")
)

// Measures
var (
	BlocksRequested         = stats.Int64("ipfs.io/bitswap/blocks_requested", "Total number of blocks requested by a peer", stats.UnitDimensionless)
	BlocksReceived          = stats.Int64("ipfs.io/bitswap/blocks_received", "Total number of blocks received by a peer", stats.UnitDimensionless)
	DuplicateBlocksReceived = stats.Int64("ipfs.io/bitswap/duplicate_blocks_received", "Total number of duplicate blocks received by a peer", stats.UnitDimensionless)
	BlocksSent              = stats.Int64("ipfs.io/bitswap/blocks_sent", "Total number of blocks sent from the peer", stats.UnitDimensionless)
	BytesSent               = stats.Float64("ipfs.io/bitswap/bytes_sent", "Total amount of bytes sent from the peer", stats.UnitBytes)
	WantListSize            = stats.Int64("ipfs.io/bitswap/wantlist_size", "Number of items in the peer's wantlist", stats.UnitDimensionless)
)

// Views
var (
	ViewBlocksRequested = &view.View{
		Measure:     BlocksRequested,
		TagKeys:     []tag.Key{KeyPeerID, KeyMessage},
		Aggregation: defaultMessageCountDistribution,
	}
	ViewBlocksReceived = &view.View{
		Measure:     BlocksReceived,
		TagKeys:     []tag.Key{KeyPeerID},
		Aggregation: defaultMessageCountDistribution,
	}
	ViewDuplicateBlocksReceived = &view.View{
		Measure:     DuplicateBlocksReceived,
		TagKeys:     []tag.Key{KeyPeerID},
		Aggregation: defaultMessageCountDistribution,
	}
	ViewBlocksSent = &view.View{
		Measure:     BlocksSent,
		TagKeys:     []tag.Key{KeyPeerID},
		Aggregation: defaultMessageCountDistribution,
	}
	ViewBytesSent = &view.View{
		Measure:     BytesSent,
		TagKeys:     []tag.Key{KeyPeerID},
		Aggregation: defaultBytesDistribution,
	}
)

// DefaultViews provides an easy to register slice of views.
var DefaultViews = []*view.View{
	ViewBlocksRequested,
	ViewBlocksReceived,
	ViewDuplicateBlocksReceived,
	ViewBlocksSent,
	ViewBytesSent,
}
