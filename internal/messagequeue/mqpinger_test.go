package messagequeue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

type mockPeerConn struct {
	err       error
	latency   time.Duration
	latencies []time.Duration
}

func (pc *mockPeerConn) Ping(ctx context.Context) ping.Result {
	timer := time.NewTimer(pc.latency)
	select {
	case <-timer.C:
		if pc.err != nil {
			return ping.Result{Error: pc.err}
		}
		pc.latencies = append(pc.latencies, pc.latency)
	case <-ctx.Done():
	}
	return ping.Result{RTT: pc.latency}
}

func (pc *mockPeerConn) Latency() time.Duration {
	sum := time.Duration(0)
	if len(pc.latencies) == 0 {
		return sum
	}
	for _, l := range pc.latencies {
		sum += l
	}
	return sum / time.Duration(len(pc.latencies))
}

func TestPingerTimeout(t *testing.T) {
	latency := time.Millisecond * 10
	latMultiplier := 2
	expProcessTime := 5 * time.Millisecond
	expectedTimeout := expProcessTime + latency*time.Duration(latMultiplier)

	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	mqp := newMQPingerWithParams(ctx, pc, dontHaveTimeout, pingInterval,
		latMultiplier, expProcessTime, maxPingErrorCount)
	mqp.Start()

	timedOut := false
	onTimeout := func() {
		timedOut = true
	}
	mqp.AfterTimeout(onTimeout)

	time.Sleep(expectedTimeout - 5*time.Millisecond)
	if timedOut {
		t.Fatal("expected timeout not to have happened yet")
	}

	time.Sleep(10 * time.Millisecond)
	if !timedOut {
		t.Fatal("expected timeout")
	}
}

func TestPingerDoestNotCreateTimersAfterShutdown(t *testing.T) {
	latency := time.Millisecond * 10
	latMultiplier := 2
	expProcessTime := 5 * time.Millisecond
	expectedTimeout := expProcessTime + latency*time.Duration(latMultiplier)

	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	mqp := newMQPingerWithParams(ctx, pc, dontHaveTimeout, pingInterval,
		latMultiplier, expProcessTime, maxPingErrorCount)

	// Give the pinger some time to start
	mqp.Start()
	time.Sleep(time.Millisecond * 5)

	// Shut down the pinger
	mqp.Shutdown()

	// Set a timer (should never fire)
	timedOut := false
	onTimeout := func() {
		timedOut = true
	}
	mqp.AfterTimeout(onTimeout)

	time.Sleep(expectedTimeout)
	if timedOut {
		t.Fatal("expected timeout not to fire")
	}
}

func TestPingerPeriodic(t *testing.T) {
	latency := time.Millisecond * 5
	// No latency padding
	latMultiplier := 1
	// No processing time
	expProcessTime := time.Duration(0)
	// No interval between pings
	pingInt := time.Duration(0)

	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	mqp := newMQPingerWithParams(ctx, pc, dontHaveTimeout, pingInt,
		latMultiplier, expProcessTime, maxPingErrorCount)
	mqp.Start()

	// Allow time for several pings
	time.Sleep(latency * 10)
	if len(pc.latencies) < 8 {
		t.Fatal("expected several pings", len(pc.latencies))
	}

	// Timeout should be the average of ping latency
	timedOut := false
	onTimeout := func() {
		timedOut = true
	}
	mqp.AfterTimeout(onTimeout)

	time.Sleep(latency / 2)
	if timedOut {
		t.Fatal("expected timeout not to have happened yet")
	}

	time.Sleep(latency)
	if !timedOut {
		t.Fatal("expected timeout")
	}

	// There should be no further pings after shutdown
	mqp.Shutdown()

	pingCount := len(pc.latencies)
	time.Sleep(latency * 5)
	if len(pc.latencies) != pingCount {
		t.Fatal("expected no more pings", len(pc.latencies))
	}
}

func TestPingerUsesDefaultTimeoutIfPingError(t *testing.T) {
	latency := time.Millisecond * 1
	latMultiplier := 2
	expProcessTime := 2 * time.Millisecond
	defaultTimeout := 10 * time.Millisecond
	expectedTimeout := expProcessTime + defaultTimeout

	ctx := context.Background()
	pc := &mockPeerConn{latency: latency, err: fmt.Errorf("ping error")}
	mqp := newMQPingerWithParams(ctx, pc, defaultTimeout, pingInterval,
		latMultiplier, expProcessTime, maxPingErrorCount)
	mqp.Start()

	timedOut := false
	onTimeout := func() {
		timedOut = true
	}
	mqp.AfterTimeout(onTimeout)

	time.Sleep(expectedTimeout - 5*time.Millisecond)
	if timedOut {
		t.Fatal("expected timeout not to have happened yet")
	}

	time.Sleep(10 * time.Millisecond)
	if !timedOut {
		t.Fatal("expected timeout")
	}
}
