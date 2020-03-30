package notifications

import (
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// PubSub is used to allow sessions to subscribe to notifications about
// incoming blocks, where multiple sessions may be interested in the same
// block.
type PubSub struct {
	lk   sync.RWMutex
	subs map[cid.Cid]map[*Subscription]struct{}
}

// Subscription is a subscription to notifications about blocks
type Subscription struct {
	lk     sync.RWMutex
	ps     *PubSub
	blks   chan blocks.Block
	closed chan struct{}
}

// New creates a new PubSub
func New() *PubSub {
	return &PubSub{
		subs: make(map[cid.Cid]map[*Subscription]struct{}),
	}
}

// Listen for keys
func (s *Subscription) Add(ks ...cid.Cid) {
	s.ps.addSubscriptionKeys(s, ks)
}

// Channel on which to receive incoming blocks. The channel should be
// closed with Close()
func (s *Subscription) Blocks() <-chan blocks.Block {
	return s.blks
}

// Receive a block
func (s *Subscription) receive(blk blocks.Block) {
	s.lk.Lock()
	defer s.lk.Unlock()

	select {
	case s.blks <- blk:
	case <-s.closed:
	}
}

// Stop listening and close the associated blocks channel
func (s *Subscription) Close() {
	close(s.closed)

	s.lk.Lock()
	defer s.lk.Unlock()

	s.ps.removeSubscription(s)

	close(s.blks)
}

// Create a new subscription to PubSub notifications
func (ps *PubSub) NewSubscription() *Subscription {
	return &Subscription{
		ps:     ps,
		blks:   make(chan blocks.Block),
		closed: make(chan struct{}),
	}
}

// Publish a block to listeners
func (ps *PubSub) Publish(blk blocks.Block) {
	ps.lk.Lock()
	defer ps.lk.Unlock()

	k := blk.Cid()
	for s := range ps.subs[k] {
		s.receive(blk)
	}
	delete(ps.subs, k)
}

// Add keys to the subscription
func (ps *PubSub) addSubscriptionKeys(s *Subscription, ks []cid.Cid) {
	ps.lk.Lock()
	defer ps.lk.Unlock()

	for _, k := range ks {
		subs, ok := ps.subs[k]
		if !ok {
			subs = make(map[*Subscription]struct{})
			ps.subs[k] = subs
		}
		subs[s] = struct{}{}
	}
}

// Remove the subscription from PubSub
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
