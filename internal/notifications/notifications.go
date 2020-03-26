package notifications

import (
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// const bufferSize = 16

// // PubSub is a sPubSube interface for publishing blocks and being able to subscribe
// // for cids. It's used internally by bitswap to decouple receiving blocks
// // and actually providing them back to the GetBlocks caller.
// type PubSub interface {
// 	Publish(block blocks.Block)
// 	// Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block
// 	NewSubscription() *Subscription
// 	// Shutdown()
// }

// New generates a new PubSub interface.
func New() *PubSub {
	return &PubSub{
		// wrapped: *pubsub.New(bufferSize),
		// closed:  make(chan struct{}),
		subs: make(map[cid.Cid]map[*Subscription]struct{}),
	}
}

type PubSub struct {
	lk   sync.RWMutex
	subs map[cid.Cid]map[*Subscription]struct{}
	// wrapped pubsub.PubSub

	// closed chan struct{}
}

func (ps *PubSub) Shutdown() {}

// func (ps *PubSub) Publish(block blocks.Block) {
// 	ps.lk.RLock()
// 	defer ps.lk.RUnlock()
// 	select {
// 	case <-ps.closed:
// 		return
// 	default:
// 	}

// 	ps.wrapped.Pub(block, block.Cid().KeyString())
// }

// func (ps *PubSub) Shutdown() {
// 	ps.lk.Lock()
// 	defer ps.lk.Unlock()
// 	select {
// 	case <-ps.closed:
// 		return
// 	default:
// 	}
// 	close(ps.closed)
// 	ps.wrapped.Shutdown()
// }

type Subscription struct {
	lk   sync.RWMutex
	ps   *PubSub
	blks chan blocks.Block
}

func (s *Subscription) Add(k cid.Cid) {
	s.lk.Lock()
	defer s.lk.Unlock()

	s.ps.addSubscriptionKey(s, k)
}

func (s *Subscription) Blocks() <-chan blocks.Block {
	s.lk.RLock()
	defer s.lk.RUnlock()

	return s.blks
}

func (s *Subscription) receive(blk blocks.Block) {
	s.lk.Lock()
	defer s.lk.Unlock()

	if s.blks != nil {
		s.blks <- blk
	}
}

func (s *Subscription) Close() {
	s.lk.Lock()
	defer s.lk.Unlock()

	s.ps.removeSubscription(s)

	select {
	case <-s.blks:
	default:
	}
	close(s.blks)
	s.blks = nil
}

func (ps *PubSub) NewSubscription() *Subscription {
	return &Subscription{
		ps:   ps,
		blks: make(chan blocks.Block),
	}
}

func (ps *PubSub) Publish(blk blocks.Block) {
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	k := blk.Cid()
	for s := range ps.subs[k] {
		s.receive(blk)
	}
	delete(ps.subs, k)
}

func (ps *PubSub) addSubscriptionKey(s *Subscription, k cid.Cid) {
	ps.lk.Lock()
	defer ps.lk.Unlock()

	subs, ok := ps.subs[k]
	if !ok {
		subs = make(map[*Subscription]struct{})
		ps.subs[k] = subs
	}
	subs[s] = struct{}{}
}

func (ps *PubSub) removeSubscription(s *Subscription) {
	ps.lk.Lock()
	defer ps.lk.Unlock()

	for k := range ps.subs {
		ksubs := ps.subs[k]
		delete(ksubs, s)
		if len(ksubs) == 0 {
			delete(ps.subs, k)
		}
	}
}

// // Subscribe returns a channel of blocks for the given |keys|. |blockChannel|
// // is closed if the |ctx| times out or is cancelled, or after receiving the blocks
// // corresponding to |keys|.
// func (ps *PubSub) Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block {

// 	blocksCh := make(chan blocks.Block, len(keys))
// 	valuesCh := make(chan interface{}, len(keys)) // provide our own channel to control buffer, prevent blocking
// 	if len(keys) == 0 {
// 		close(blocksCh)
// 		return blocksCh
// 	}

// 	// prevent shutdown
// 	ps.lk.RLock()
// 	defer ps.lk.RUnlock()

// 	select {
// 	case <-ps.closed:
// 		close(blocksCh)
// 		return blocksCh
// 	default:
// 	}

// 	// AddSubOnceEach listens for each key in the list, and closes the channel
// 	// once all keys have been received
// 	ps.wrapped.AddSubOnceEach(valuesCh, toStrings(keys)...)
// 	go func() {
// 		defer func() {
// 			close(blocksCh)

// 			ps.lk.RLock()
// 			defer ps.lk.RUnlock()
// 			// Don't touch the pubsub instance if we're
// 			// already closed.
// 			select {
// 			case <-ps.closed:
// 				return
// 			default:
// 			}

// 			ps.wrapped.Unsub(valuesCh)
// 		}()

// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case <-ps.closed:
// 			case val, ok := <-valuesCh:
// 				if !ok {
// 					return
// 				}
// 				block, ok := val.(blocks.Block)
// 				if !ok {
// 					return
// 				}
// 				select {
// 				case <-ctx.Done():
// 					return
// 				case blocksCh <- block: // continue
// 				case <-ps.closed:
// 				}
// 			}
// 		}
// 	}()

// 	return blocksCh
// }

// func toStrings(keys []cid.Cid) []string {
// 	strs := make([]string, 0, len(keys))
// 	for _, key := range keys {
// 		strs = append(strs, key.KeyString())
// 	}
// 	return strs
// }
