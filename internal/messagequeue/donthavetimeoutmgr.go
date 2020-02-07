package messagequeue

import (
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

const (
	// dontHaveTimeout is used to simulate a DONT_HAVE when communicating with
	// a peer whose Bitswap client doesn't support the DONT_HAVE response.
	// If the peer doesn't respond to a want-block within the timeout, the
	// local node assumes that the peer doesn't have the block.
	dontHaveTimeout = 5 * time.Second

	// maxExpectedWantProcessTime is the maximum amount of time we expect a
	// peer takes to process a want and initiate sending a response to us
	maxExpectedWantProcessTime = 200 * time.Millisecond

	// latencyMultiplier is multiplied by the average ping time to
	// get an upper bound on how long we expect to wait for a peer's response
	// to arrive
	latencyMultiplier = 2
)

// PeerConnection is a connection to a peer that can be pinged, and the
// average latency measured
type PeerConnection interface {
	// Ping the peer
	Ping(context.Context) ping.Result
	// The average latency of all pings
	Latency() time.Duration
}

// dontHaveTimeoutMgr pings the peer to measure latency. It uses the latency to
// set a reasonable timeout for simulating a DONT_HAVE message for peers that
// don't support DONT_HAVE
type dontHaveTimeoutMgr struct {
	ctx                        context.Context
	shutdown                   func()
	peerConn                   PeerConnection
	onDontHaveTimeout          func([]cid.Cid)
	defaultTimeout             time.Duration
	latencyMultiplier          int
	maxExpectedWantProcessTime time.Duration

	// All variables below here must be proctected by the lock
	lk                    sync.RWMutex
	started               bool
	pending               map[cid.Cid]time.Time
	timeout               time.Duration
	checkForTimeoutsTimer *time.Timer
}

// newDontHaveTimeoutMgr creates a new dontHaveTimeoutMgr
// onDontHaveTimeout is called when pending keys expire (not cancelled before timeout)
func newDontHaveTimeoutMgr(ctx context.Context, pc PeerConnection, onDontHaveTimeout func([]cid.Cid)) *dontHaveTimeoutMgr {
	return newDontHaveTimeoutMgrWithParams(ctx, pc, onDontHaveTimeout, dontHaveTimeout,
		latencyMultiplier, maxExpectedWantProcessTime)
}

// newDontHaveTimeoutMgrWithParams is used by the tests
func newDontHaveTimeoutMgrWithParams(ctx context.Context, pc PeerConnection, onDontHaveTimeout func([]cid.Cid),
	defaultTimeout time.Duration, latencyMultiplier int,
	maxExpectedWantProcessTime time.Duration) *dontHaveTimeoutMgr {

	ctx, shutdown := context.WithCancel(ctx)
	mqp := &dontHaveTimeoutMgr{
		ctx:                        ctx,
		shutdown:                   shutdown,
		peerConn:                   pc,
		pending:                    make(map[cid.Cid]time.Time),
		timeout:                    defaultTimeout,
		defaultTimeout:             defaultTimeout,
		latencyMultiplier:          latencyMultiplier,
		maxExpectedWantProcessTime: maxExpectedWantProcessTime,
		onDontHaveTimeout:          onDontHaveTimeout,
	}

	return mqp
}

// Shutdown the dontHaveTimeoutMgr. Any subsequent call to Start() will be ignored
func (dhtm *dontHaveTimeoutMgr) Shutdown() {
	dhtm.shutdown()
}

// onShutdown is called when the dontHaveTimeoutMgr shuts down
func (dhtm *dontHaveTimeoutMgr) onShutdown() {
	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Clear any pending check for timeouts
	if dhtm.checkForTimeoutsTimer != nil {
		dhtm.checkForTimeoutsTimer.Stop()
	}
}

// onStarted is called when the dontHaveTimeoutMgr starts.
// It monitors for the context being cancelled.
func (dhtm *dontHaveTimeoutMgr) onStarted() {
	<-dhtm.ctx.Done()
	dhtm.onShutdown()
}

// Start the dontHaveTimeoutMgr. This method is idempotent
func (dhtm *dontHaveTimeoutMgr) Start() {
	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Make sure the dont have timeout manager hasn't already been started
	if dhtm.started {
		return
	}
	dhtm.started = true

	go dhtm.onStarted()

	// If we already have a measure of latency to the peer, use it to
	// calculate a reasonable timeout
	latency := dhtm.peerConn.Latency()
	if latency.Nanoseconds() > 0 {
		dhtm.timeout = dhtm.calculateTimeoutFromLatency(latency)
		return
	}

	// Otherwise measure latency by pinging the peer
	go dhtm.measureLatency()
}

// measureLatency measures the latency to the peer by pinging it
func (dhtm *dontHaveTimeoutMgr) measureLatency() {
	// Wait up to defaultTimeout for a response to the ping
	ctx, cancel := context.WithTimeout(dhtm.ctx, dhtm.defaultTimeout)
	defer cancel()

	// Ping the peer
	res := dhtm.peerConn.Ping(ctx)
	if res.Error != nil {
		// If there was an error, we'll just leave the timeout as
		// defaultTimeout
		return
	}

	// Get the average latency to the peer
	latency := dhtm.peerConn.Latency()

	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Calculate a reasonable timeout based on latency
	dhtm.timeout = dhtm.calculateTimeoutFromLatency(latency)

	// Check if after changing the timeout there are any pending wants that are
	// now over the timeout
	go dhtm.checkForTimeouts()
}

// checkForTimeouts checks pending wants to see if any are over the timeout
func (dhtm *dontHaveTimeoutMgr) checkForTimeouts() {
	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	if len(dhtm.pending) == 0 {
		return
	}

	// Figure out which of the blocks that were wanted were not received
	// within the timeout
	pending := make([]cid.Cid, 0, len(dhtm.pending))
	var oldest time.Time
	for c, started := range dhtm.pending {
		if time.Since(started) > dhtm.timeout {
			pending = append(pending, c)
			delete(dhtm.pending, c)
		} else if oldest.IsZero() || started.Before(oldest) {
			oldest = started
		}
	}

	// Fire the timeout event for the pending wants
	if len(pending) > 0 {
		go dhtm.fireTimeout(pending)
	}

	if len(dhtm.pending) == 0 {
		return
	}

	// Make sure things are still running
	select {
	case <-dhtm.ctx.Done():
		return
	default:
	}

	// If there's an existing scheduled check, stop it
	if dhtm.checkForTimeoutsTimer != nil {
		dhtm.checkForTimeoutsTimer.Stop()
	}

	// Schedule the next check for the moment when the oldest pending want will
	// timeout
	until := time.Until(oldest.Add(dhtm.timeout))
	dhtm.checkForTimeoutsTimer = time.AfterFunc(until, dhtm.checkForTimeouts)
}

// AddPending adds the given keys that will expire if not cancelled before
// the timeout
func (dhtm *dontHaveTimeoutMgr) AddPending(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}

	start := time.Now()

	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	queueWasEmpty := len(dhtm.pending) == 0

	// Record the start time for each key
	for _, c := range ks {
		if _, ok := dhtm.pending[c]; !ok {
			dhtm.pending[c] = start
		}
	}

	// If there was already an earlier pending item in the queue, then there
	// must already be a timeout check scheduled. If there is nothing in the
	// queue then we should make sure to schedule a check.
	if queueWasEmpty {
		go dhtm.checkForTimeouts()
	}
}

// CancelPending is called when we receive a response for a key
func (dhtm *dontHaveTimeoutMgr) CancelPending(ks []cid.Cid) {
	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// The want has been cancelled, so remove pending timers
	for _, c := range ks {
		delete(dhtm.pending, c)
	}
}

// fireTimeout fires the onDontHaveTimeout method with the timed out keys
func (dhtm *dontHaveTimeoutMgr) fireTimeout(pending []cid.Cid) {
	// Make sure the timeout manager has not been shut down
	select {
	case <-dhtm.ctx.Done():
		return
	default:
	}

	// Fire the timeout
	dhtm.onDontHaveTimeout(pending)
}

// calculateTimeoutFromLatency calculates a reasonable timeout derived from latency
func (dhtm *dontHaveTimeoutMgr) calculateTimeoutFromLatency(latency time.Duration) time.Duration {
	// The maximum expected time for a response is
	// the expected time to process the want + (latency * multiplier)
	// The multiplier is to provide some padding for variable latency.
	return dhtm.maxExpectedWantProcessTime + time.Duration(dhtm.latencyMultiplier)*latency
}
