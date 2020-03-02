package peertimeoutmanager

import (
	"context"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"

	bstimer "github.com/ipfs/go-bitswap/internal/timer"
)

const (
	// The time to allow a peer to respond before assuming it is unresponsive.
	peerResponseTimeout = 10 * time.Second

	// We keep track of requests in a map, and the order of requests in an
	// array. As responses come in items are deleted from the map and set to
	// inactive in the array. If the array and map get too far out of sync
	// we pause to filter the array. requestGCCount is the maximum allowed
	// difference between map and array length before forcing a filtering.
	requestGCCount = 256
)

type request struct {
	// The peer the request was sent to
	p peer.ID
	// The time at which the request was sent
	sent time.Time
	// active is true until a response is received
	active bool
}

// FnPeerResponseTimeout is called when one or more peers time out
type FnPeerResponseTimeout func([]peer.ID)

// PeerTimeoutManager keeps track of requests and responses to peers.
// It reports a timeout if there is no response from a peer to any request
// for some period of time.
type PeerTimeoutManager struct {
	ctx                   context.Context
	peerResponseTimeout   time.Duration
	onPeerResponseTimeout FnPeerResponseTimeout
	// queue of requests that are sent
	requestSent chan request
	// queue of responding peers
	responseReceived chan peer.ID
}

// New creates a new PeerTimeoutManager.
func New(ctx context.Context, onTimeout FnPeerResponseTimeout) *PeerTimeoutManager {
	return newPeerTimeoutManager(ctx, onTimeout, peerResponseTimeout)
}

// Used by the tests
func newPeerTimeoutManager(ctx context.Context, onTimeout FnPeerResponseTimeout, timeout time.Duration) *PeerTimeoutManager {
	ptm := &PeerTimeoutManager{
		ctx:                   ctx,
		peerResponseTimeout:   timeout,
		onPeerResponseTimeout: onTimeout,
		requestSent:           make(chan request, 128),
		responseReceived:      make(chan peer.ID, 128),
	}

	go ptm.run()

	return ptm
}

// Called when a request is sent to a peer
func (ptm *PeerTimeoutManager) RequestSent(p peer.ID) {
	select {
	case ptm.requestSent <- request{p: p, sent: time.Now(), active: true}:
	case <-ptm.ctx.Done():
	}
}

// Called when a response is received from a peer
func (ptm *PeerTimeoutManager) ResponseReceived(p peer.ID) {
	select {
	case ptm.responseReceived <- p:
	case <-ptm.ctx.Done():
	}
}

// The main run loop for the class
func (ptm *PeerTimeoutManager) run() {
	// Create a timer to detect when a peer doesn't respond for too long
	responseTimer := time.NewTimer(0)
	defer responseTimer.Stop()
	bstimer.StopTimer(responseTimer)

	// Keep track of requests sent to a peer, and the order of requests
	requestsSent := make(map[peer.ID]*request)
	var inOrder []*request
	for {
		select {
		case rq := <-ptm.requestSent:
			// Check if there was an earlier request sent to the peer
			if _, ok := requestsSent[rq.p]; !ok {
				// This is the first request sent to the peer
				requestsSent[rq.p] = &rq
				inOrder = append(inOrder, &rq)

				// If this is the first request sent to any peer, set a timer
				// to expire on timeout
				if len(requestsSent) == 1 {
					responseTimer.Reset(ptm.peerResponseTimeout)
				}
			}

		case from := <-ptm.responseReceived:
			// A response was received so mark any request to the peer as no
			// longer active
			if rq, ok := requestsSent[from]; ok {
				rq.active = false
				delete(requestsSent, from)

				// If a lot of responses have been received, clean up the array
				// that keeps track of response order
				if len(inOrder)-len(requestsSent) > requestGCCount {
					inOrder = removeInactive(inOrder)
				}
			}

		case <-responseTimer.C:
			// The timeout has expired, so remove expired requests from the
			// queue
			expired := make([]peer.ID, 0, len(requestsSent))
			for len(inOrder) > 0 {
				rq := inOrder[0]

				// If the request is still active
				if rq.active {
					// The queue is in order from earliest to latest, so if we
					// didn't find an expired entry we can stop iterating
					if time.Since(rq.sent) < ptm.peerResponseTimeout {
						break
					}

					// Add the peer to the expired list
					expired = append(expired, rq.p)
					// Remove the request from the requestsSent map
					delete(requestsSent, rq.p)
				}

				// Remove expired or inactive requests from the queue
				inOrder = inOrder[1:]
			}

			// Fire the timeout event for peers that have timed out
			if len(expired) > 0 {
				go ptm.fireTimeout(expired)
			}

			// If there are some active requests, and the manager hasn't
			// shut down
			if len(requestsSent) > 0 && ptm.ctx.Err() == nil {
				// Schedule the next check for the moment when the oldest
				// active request will timeout
				oldestStart := inOrder[0].sent
				until := time.Until(oldestStart.Add(ptm.peerResponseTimeout))
				responseTimer.Reset(until)
			}

		case <-ptm.ctx.Done():
			return
		}
	}
}

func removeInactive(rqs []*request) []*request {
	to := 0
	for from := 0; from < len(rqs); from++ {
		if rqs[from].active {
			rqs[to] = rqs[from]
			to++
		}
	}

	// Ensure the Garbage Collector can clean up the underlying
	// array elements by setting them to nil
	for i := to; i < len(rqs); i++ {
		rqs[i] = nil
	}

	// Truncate the slice
	rqs = rqs[:to]

	return rqs
}

// fireTimeout calls the callback with the timed out peers
func (ptm *PeerTimeoutManager) fireTimeout(peers []peer.ID) {
	// Make sure the peer timeout manager has not been shut down
	if ptm.ctx.Err() != nil {
		return
	}

	// Fire the timeout
	ptm.onPeerResponseTimeout(peers)
}
