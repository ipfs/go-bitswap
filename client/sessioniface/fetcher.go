package sessioniface

import (
	"context"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
)

type AddRemoveCid interface {
	IsAdd() bool
	Key() cid.Cid
}

type ChannelFetcher interface {
	exchange.Fetcher
	GetBlocksCh(ctx context.Context, keys <-chan AddRemoveCid) (<-chan blocks.Block, error)
}
