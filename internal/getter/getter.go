package getter

import (
	"context"
	"errors"

	notifications "github.com/ipfs/go-bitswap/internal/notifications"
	logging "github.com/ipfs/go-log"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
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
type WantFunc func([]cid.Cid)

// AsyncGetBlocks listens for the blocks corresponding to the requested wants,
// and outputs them on the returned channel.
// If the wants channel is closed and all wanted blocks are received, closes
// the returned channel.
// If the session context or request context are cancelled, calls cancelWants
// with all pending wants and closes the returned channel.
func AsyncGetBlocks(ctx context.Context, sessctx context.Context, wants <-chan []cid.Cid, notif *notifications.PubSub,
	want WantFunc, cancelWants func([]cid.Cid)) (<-chan blocks.Block, error) {

	// Channel of blocks to return to the client
	out := make(chan blocks.Block)

	// Keep track of which wants we haven't yet received a block
	pending := cid.NewSet()

	// Use a PubSub notifier to listen for incoming blocks for each key
	sub := notif.NewSubscription()

	go func() {
		// Before exiting
		defer func() {
			// Close the client's channel of blocks
			close(out)
			// Close the subscription
			sub.Close()

			// Cancel any pending wants
			if pending.Len() > 0 {
				cancelWants(pending.Keys())
			}
		}()

		blksCh := sub.Blocks()
		for {
			select {

			// For each wanted key
			case ks, ok := <-wants:
				// Stop receiving from the channel if it's closed
				if !ok {
					wants = nil
					if pending.Len() == 0 {
						return
					}
				} else {
					for _, k := range ks {
						// Record that the want is pending
						log.Debugw("Bitswap.GetBlockRequest.Start", "cid", k)
						pending.Add(k)
					}

					// Add the keys to the subscriber so that we'll be notified
					// if the corresponding block arrives
					sub.Add(ks...)

					// Send the want request for the keys to the network
					want(ks)
				}

			// For each received block
			case blk := <-blksCh:
				// Remove the want from the pending set
				pending.Remove(blk.Cid())

				// Send the block to the client
				select {
				case out <- blk:
				case <-ctx.Done():
					return
				case <-sessctx.Done():
					return
				}

				// If the wants channel has been closed, and we're not
				// expecting any more blocks, exit
				if wants == nil && pending.Len() == 0 {
					return
				}

			case <-ctx.Done():
				return
			case <-sessctx.Done():
				return
			}
		}
	}()

	return out, nil
}
