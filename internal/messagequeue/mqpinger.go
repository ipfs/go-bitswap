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

	// wantProcessLatencyMultiplier is multiplied by the average ping time to
	// get an upper bound on how long we expect to wait for a peer's response
	// to arrive
	wantProcessLatencyMultiplier = 2

	// maxPingErrorCount is the maximum number of times we can receive an
	// error while pinging before we give up
	maxPingErrorCount = 3

	// pingInterval is the time between pings
	pingInterval = 60 * time.Second
)

type PeerConnection interface {
	Ping(context.Context) ping.Result
	Latency() time.Duration
}

type Stoppable interface {
	Stop() bool
}

type mqPinger struct {
	ctx                        context.Context
	stop                       func()
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

func newMQPinger(ctx context.Context, pc PeerConnection, defaultTimeout time.Duration) *mqPinger {
	return newMQPingerWithParams(ctx, pc, defaultTimeout, pingInterval,
		wantProcessLatencyMultiplier, maxExpectedWantProcessTime, maxPingErrorCount)
}

func newMQPingerWithParams(ctx context.Context, pc PeerConnection,
	defaultTimeout time.Duration, pingInterval time.Duration, latencyMultiplier int,
	maxExpectedWantProcessTime time.Duration, maxErrorCount int) *mqPinger {

	ctx, stop := context.WithCancel(ctx)
	mqp := &mqPinger{
		ctx:                        ctx,
		stop:                       stop,
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

type pendingTimer struct {
	fn    func()
	start time.Time
	lk    sync.Mutex
	timer *time.Timer
}

func (pt *pendingTimer) Stop() bool {
	pt.lk.Lock()
	defer pt.lk.Unlock()

	if pt.timer != nil {
		return pt.timer.Stop()
	}
	return true
}

func (mqp *mqPinger) AfterTimeout(fn func()) Stoppable {
	pt := &pendingTimer{fn: fn, start: time.Now()}

	select {
	case mqp.pendingTimers <- pt:
	case <-mqp.ctx.Done():
	}

	return pt
}

func (mqp *mqPinger) Stop() {
	mqp.stop()
}

func (mqp *mqPinger) Start() {
	select {
	case mqp.startCh <- struct{}{}:
	case <-mqp.ctx.Done():
	default:
	}
}

func (mqp *mqPinger) init() {
	defer mqp.shutdown()

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

	if doPeriodicPing {
		go mqp.periodicPing()
	}

	mqp.run()
}

func (mqp *mqPinger) shutdown() {
	// TODO: Do I need to drain the pending timer / startCh queues?
}

func (mqp *mqPinger) start() bool {
	res := mqp.peerConn.Ping(mqp.ctx)
	if res.Error != nil {
		mqp.rtt = mqp.defaultTimeout
		return false
	}

	mqp.rtt = mqp.peerConn.Latency()
	return true
}

func (mqp *mqPinger) periodicPing() {
	errCnt := 0
	periodicTick := time.NewTimer(mqp.pingInterval)
	defer periodicTick.Stop()

	for {
		select {
		case <-periodicTick.C:
			res := mqp.peerConn.Ping(mqp.ctx)
			if res.Error != nil {
				errCnt++
				if errCnt > mqp.maxErrorCount {
					return
				}
			}
		case <-mqp.ctx.Done():
			return
		}
	}
}

func (mqp *mqPinger) run() {
	for {
		select {
		case pt := <-mqp.pendingTimers:
			mqp.createTimer(pt)
		case <-mqp.ctx.Done():
			return
		}
	}
}

func (mqp *mqPinger) createTimer(pt *pendingTimer) {
	expected := mqp.maxExpectedWantProcessTime + time.Duration(mqp.latencyMultiplier)*mqp.peerConn.Latency()
	after := expected - time.Since(pt.start)

	pt.lk.Lock()
	defer pt.lk.Unlock()

	pt.timer = time.AfterFunc(after, pt.fn)
}
