package session

import (
	"testing"
	"time"
)

func TestLatencyTracker(t *testing.T) {
	lt := newLatencyTracker(0.25)

	if lt.hasLatency() {
		t.Fatal("expected not to have latency yet")
	}

	if lt.smoothedLatency() != 0 {
		t.Fatal("expected not to have latency yet")
	}

	lt.receiveUpdate(3, time.Duration(600*time.Millisecond))

	if !lt.hasLatency() {
		t.Fatal("expected to have latency")
	}

	// 600ms / 3
	if lt.smoothedLatency() != time.Duration(200*time.Millisecond) {
		t.Fatal("unexpected latency", lt.smoothedLatency())
	}

	lt.receiveUpdate(1, time.Duration(100*time.Millisecond))

	// 200ms * 0.5 + 100ms * 0.5
	if lt.smoothedLatency() != time.Duration(150*time.Millisecond) {
		t.Fatal("unexpected latency", lt.smoothedLatency())
	}

	lt.receiveUpdate(1, time.Duration(100*time.Millisecond))

	// 150ms * 0.66 + 100ms * 0.33
	if lt.smoothedLatency().Milliseconds() != 133 {
		t.Fatal("unexpected latency", lt.smoothedLatency())
	}

	lt.receiveUpdate(1, time.Duration(1000*time.Millisecond))

	// 133ms * 0.75 + 1000ms * 0.25
	if lt.smoothedLatency().Milliseconds() != 350 {
		t.Fatal("unexpected latency", lt.smoothedLatency())
	}

	// From this point on alpha clamps to 0.25

	lt.receiveUpdate(1, time.Duration(102*time.Millisecond))

	// 350ms * 0.75 + 102ms * 0.25
	if lt.smoothedLatency().Milliseconds() != 288 {
		t.Fatal("unexpected latency", lt.smoothedLatency())
	}

	lt.receiveUpdate(1, time.Duration(100*time.Millisecond))

	// 288ms * 0.75 + 100ms * 0.25
	if lt.smoothedLatency().Milliseconds() != 241 {
		t.Fatal("unexpected latency", lt.smoothedLatency())
	}
}
