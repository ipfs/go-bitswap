package providerqueryer

import (
	"context"
	"reflect"
	"sync"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

// for overriding in tests
var newMutex = func() sync.Locker { return &sync.Mutex{} }
var newRWMutex = func() rwLocker { return &sync.RWMutex{} }

// request keeps track of the receivers for a CID.
// Concurrent requests for the same CID are tracked here and new receivers are backfilled
// with previously-found providers.
type request struct {
	m         sync.Locker
	found     []peer.ID
	receivers []chan peer.ID
	closed    bool

	// Incremented when something may still be sending to a receiver.
	// This prevents Close() from closing channels that are in use.
	// This should only be used when the lock is held.
	receivingWG sync.WaitGroup
}

func (r *request) getReceivers() []chan peer.ID {
	rs := make([]chan peer.ID, len(r.receivers))
	copy(rs, r.receivers)
	return rs
}

func (r *request) getFound() []peer.ID {
	f := make([]peer.ID, len(r.found))
	copy(f, r.found)
	return f
}

// AddReceiver adds a receiver channel for this CID.
// This is called concurrently by incoming FindProvidersAsync calls.
func (r *request) AddReceiver(ctx context.Context, ch chan peer.ID) (isFirst bool) {
	r.m.Lock()

	closed := r.closed
	found := r.getFound()

	isFirst = !closed && len(r.receivers) == 0

	// The waitgroup protects the channel from being closed by Close() while it's being backfilled.
	// Note that if any AddReceiver() calls arrive after Close() has acquired the lock,
	// then this is fine--AddReceiver() will backfill the channel.
	r.receivingWG.Add(1)
	defer r.receivingWG.Done()
	r.receivers = append(r.receivers, ch)

	r.m.Unlock()

	// Backfill the channel with providers we've already found.
	for _, p := range found {
		select {
		case ch <- p:
		case <-ctx.Done():
			return
		}
	}

	// If we're adding a receiver to a request that's already closed,
	// which can happen if requests.Close() and requests.Add() race,
	// then close the door behind us since there will be no more notifications
	// and no more opportunities to close the channel.
	if closed {
		close(ch)
	}

	return
}

// Notify notifies all receivers that the given peer is a provider for the CID.
// This is called concurrently by the first FindProvidersAsync goroutine for the CID,
// whenever a new provider is found.
func (r *request) Notify(ctx context.Context, p peer.ID) {
	r.m.Lock()
	r.found = append(r.found, p)
	receivers := r.getReceivers()
	r.receivingWG.Add(1)
	defer r.receivingWG.Done()
	r.m.Unlock()

	// Select over all the channels, to avoid head-of-line blocking.
	cases := make([]reflect.SelectCase, len(receivers))
	pVal := reflect.ValueOf(p)
	for i, ch := range receivers {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: reflect.ValueOf(ch),
			Send: pVal,
		}
	}

	for len(cases) != 0 {
		chosen, _, _ := reflect.Select(cases)
		cases[chosen] = cases[len(cases)-1]
		cases = cases[:len(cases)-1]
	}
}

// Close closes all receiver channels.
// No Notify() calls should be issued after this is called, although AddReceiver() may still be called.
// This is called exactly once by the first FindProvidersAsync goroutine for the CID.
func (r *request) Close() {
	// Acquire the lock, then wait on any ongoing receives to finish.
	// We hold the lock while waiting to prevent any concurrent AddReceiver requests from starting new work
	// until the request is closed.
	r.m.Lock()
	defer r.m.Unlock()
	// Wait for any ongoing notifications to finish before closing.
	// At this point, the req has been removed the requests map,
	// so there will be no new incoming requests after the wg.Wait().
	r.receivingWG.Wait()
	for _, ch := range r.receivers {
		close(ch)
	}
	r.receivers = nil
	r.closed = true
}

// requests keeps track of ongoing requests by CID.
type requests struct {
	m    rwLocker
	rMap map[cid.Cid]*request
}

// AddReceiverOrCreateRequest adds a new receiver for the given CID.
// If there is already a query running for the CID, any already-found providers
// are sent to the channel synchronously as part of this call.
func (r *requests) AddReceiverOrCreateRequest(ctx context.Context, k cid.Cid, ch chan peer.ID) (isFirst bool, req *request) {
	r.m.Lock()
	req, ok := r.rMap[k]
	if !ok {
		req = &request{m: newMutex()}
		r.rMap[k] = req
	}
	r.m.Unlock()

	isFirst = req.AddReceiver(ctx, ch)
	return
}

func (r *requests) RequestDone(k cid.Cid) {
	r.m.Lock()
	delete(r.rMap, k)
	r.m.Unlock()
}
