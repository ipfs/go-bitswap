package messagequeue

import (
	"context"
	"sync"
	"time"

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

	// maxPingErrorCount is the maximum number of times we can receive an
	// error while pinging before we give up
	maxPingErrorCount = 3

	// pingInterval is the time between pings
	pingInterval = 60 * time.Second
)

// PeerConnection is a connection to a peer that can be pinged, and the
// average latency measured
type PeerConnection interface {
	// Ping the peer
	Ping(context.Context) ping.Result
	// The average latency of all pings
	Latency() time.Duration
}

// Stoppable can be stopped
type Stoppable interface {
	// Stop the Stoppable - the return value is false if it was already stopped.
	Stop() bool
}

// mqPinger pings the peer to measure latency. It uses the latency to set a
// reasonable timeout for AfterTimeout().
type mqPinger struct {
	ctx                        context.Context
	shutdown                   func()
	peerConn                   PeerConnection
	startCh                    chan struct{}
	pendingTimers              chan *pendingTimer
	rtt                        time.Duration
	defaultTimeout             time.Duration
	pingInterval               time.Duration
	latencyMultiplier          int
	maxExpectedWantProcessTime time.Duration
	maxErrorCount              int
}

// newMQPinger creates a new mqPinger
func newMQPinger(ctx context.Context, pc PeerConnection) *mqPinger {
	return newMQPingerWithParams(ctx, pc, dontHaveTimeout, pingInterval,
		latencyMultiplier, maxExpectedWantProcessTime, maxPingErrorCount)
}

// newMQPingerWithParams is used by the tests
func newMQPingerWithParams(ctx context.Context, pc PeerConnection,
	defaultTimeout time.Duration, pingInterval time.Duration, latencyMultiplier int,
	maxExpectedWantProcessTime time.Duration, maxErrorCount int) *mqPinger {

	ctx, shutdown := context.WithCancel(ctx)
	mqp := &mqPinger{
		ctx:                        ctx,
		shutdown:                   shutdown,
		peerConn:                   pc,
		startCh:                    make(chan struct{}, 1),
		pendingTimers:              make(chan *pendingTimer, 128),
		defaultTimeout:             defaultTimeout,
		pingInterval:               pingInterval,
		latencyMultiplier:          latencyMultiplier,
		maxExpectedWantProcessTime: maxExpectedWantProcessTime,
		maxErrorCount:              maxErrorCount,
	}

	go mqp.init()

	return mqp
}

// pendingTimer keeps information required for a timer that has been craeted
// but not yet processed
type pendingTimer struct {
	fn    func()
	start time.Time
	lk    sync.Mutex
	timer *time.Timer
}

// Stop the timer
func (pt *pendingTimer) Stop() bool {
	pt.lk.Lock()
	defer pt.lk.Unlock()

	// If the timer has been started, stop it
	if pt.timer != nil {
		return pt.timer.Stop()
	}
	return false
}

// AfterTimeout calls the given function after a reasonable estimate of when
// the peer should be expected to respond by. The estimate is derived from
// the ping latency plus some padding for processing time.
func (mqp *mqPinger) AfterTimeout(fn func()) Stoppable {
	pt := &pendingTimer{fn: fn, start: time.Now()}

	select {
	case mqp.pendingTimers <- pt:
	case <-mqp.ctx.Done():
	}

	return pt
}

// Shutdown the mqPinger. Any subsequent call to Start() will be ignored
func (mqp *mqPinger) Shutdown() {
	mqp.shutdown()
}

// Start the mqPinger
func (mqp *mqPinger) Start() {
	select {
	case mqp.startCh <- struct{}{}:
	case <-mqp.ctx.Done():
	default:
	}
}

// init the mqPinger
func (mqp *mqPinger) init() {
	defer mqp.onShutdown()

	// Ping the peer. If there's an error don't do periodic pings, just use
	// the default timeout.
	var doPeriodicPing bool
	select {
	case <-mqp.ctx.Done():
		return
	case <-mqp.startCh:
		doPeriodicPing = mqp.start()
	}

	select {
	case <-mqp.ctx.Done():
		return
	default:
	}

	// If doing periodic pings, start pinging
	if doPeriodicPing {
		go mqp.periodicPing()
	}

	// Process incoming timers
	mqp.processTimers()
}

// onShutdown is called when the mqPinger shuts down
func (mqp *mqPinger) onShutdown() {
	// TODO: Do I need to drain the pending timer / startCh queues?
}

// start pings the peer and sets the RTT. If there's an error, it derives the
// RTT from the default timeout and returns false.
func (mqp *mqPinger) start() bool {
	ctx, cancel := context.WithTimeout(mqp.ctx, mqp.defaultTimeout)
	defer cancel()
	res := mqp.peerConn.Ping(ctx)
	if res.Error != nil {
		mqp.rtt = mqp.defaultTimeout / time.Duration(mqp.latencyMultiplier)
		return false
	}

	mqp.rtt = mqp.peerConn.Latency()
	return true
}

// Ping the peer periodically
func (mqp *mqPinger) periodicPing() {
	errCnt := 0
	timer := time.NewTimer(mqp.pingInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Ping the peer
			res := mqp.peerConn.Ping(mqp.ctx)
			if res.Error != nil {
				errCnt++
				// If there are too many errors, stop pinging
				if errCnt > mqp.maxErrorCount {
					return
				}
			}
			timer = time.NewTimer(mqp.pingInterval)
		case <-mqp.ctx.Done():
			return
		}
	}
}

// processTimers pulls timer requests of the queue and starts them
func (mqp *mqPinger) processTimers() {
	for {
		select {
		case pt := <-mqp.pendingTimers:
			mqp.createTimer(pt)
		case <-mqp.ctx.Done():
			return
		}
	}
}

// createTimer turns a pendingTimer into a timer
func (mqp *mqPinger) createTimer(pt *pendingTimer) {
	// The maximum expected time for a response is
	// the expected time to process the want + (latency * multiplier)
	// The multiplier is to provide some padding for variable latency.
	expected := mqp.maxExpectedWantProcessTime + time.Duration(mqp.latencyMultiplier)*mqp.rtt
	after := expected - time.Since(pt.start)

	pt.lk.Lock()
	defer pt.lk.Unlock()

	pt.timer = time.AfterFunc(after, pt.fn)
}
