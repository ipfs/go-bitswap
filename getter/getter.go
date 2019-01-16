package getter

import (
	"context"
	"errors"

	logging "github.com/ipfs/go-log"

	notifications "github.com/ipfs/go-bitswap/notifications"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"go.opencensus.io/trace"
)

var log = logging.Logger("bitswap")

// GetBlocksFunc is any function that can take an array of CIDs and return a
// channel of incoming blocks.
type GetBlocksFunc func(context.Context, []cid.Cid) (<-chan blocks.Block, error)

// SyncGetBlock takes a block cid and an async function for getting several
// blocks that returns a channel, and uses that function to return the
// block syncronously.
func SyncGetBlock(p context.Context, k cid.Cid, gb GetBlocksFunc) (blocks.Block, error) {
	if !k.Defined() {
		log.Error("undefined cid in GetBlock")
		return nil, blockstore.ErrNotFound
	}

	// Any async work initiated by this function must end when this function
	// returns. To ensure this, derive a new context. Note that it is okay to
	// listen on parent in this scope, but NOT okay to pass |parent| to
	// functions called by this one. Otherwise those functions won't return
	// when this context's cancel func is executed. This is difficult to
	// enforce. May this comment keep you safe.
	ctx, cancel := context.WithCancel(p)
	defer cancel()

	promise, err := gb(ctx, []cid.Cid{k})
	if err != nil {
		return nil, err
	}

	select {
	case block, ok := <-promise:
		if !ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, errors.New("promise channel was closed")
			}
		}
		return block, nil
	case <-p.Done():
		return nil, p.Err()
	}
}

// WantFunc is any function that can express a want for set of blocks.
type WantFunc func(context.Context, []cid.Cid)

// AsyncGetBlocks take a set of block cids, a pubsub channel for incoming
// blocks, a want function, and a close function,
// and returns a channel of incoming blocks.
func AsyncGetBlocks(ctx context.Context, keys []cid.Cid, notif notifications.PubSub, want WantFunc, cwants func([]cid.Cid)) (<-chan blocks.Block, error) {
	if len(keys) == 0 {
		out := make(chan blocks.Block)
		close(out)
		return out, nil
	}

	span := trace.FromContext(ctx)
	remaining := cid.NewSet()
	promise := notif.Subscribe(ctx, keys...)
	for _, k := range keys {
		if span != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("cid", k.String()),
			}, "Bitswap.GetBlockRequest.Start")
		}
		remaining.Add(k)
	}

	want(ctx, keys)

	out := make(chan blocks.Block)
	go handleIncoming(ctx, remaining, promise, out, cwants)
	return out, nil
}

func handleIncoming(ctx context.Context, remaining *cid.Set, in <-chan blocks.Block, out chan blocks.Block, cfun func([]cid.Cid)) {
	ctx, cancel := context.WithCancel(ctx)
	span := trace.FromContext(ctx)
	defer func() {
		cancel()
		close(out)
		// can't just defer this call on its own, arguments are resolved *when* the defer is created
		cfun(remaining.Keys())
	}()
	for {
		select {
		case blk, ok := <-in:
			if !ok {
				return
			}

			remaining.Remove(blk.Cid())
			if span != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("cid", blk.Cid().String()),
				}, "Bitswap.GetBlockRequest.End")
			}
			select {
			case out <- blk:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
