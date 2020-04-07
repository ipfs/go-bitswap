package session

import (
	"time"
)

const latencyTrackerAlpha = 0.25

// latencyTracker keeps track of an EWMA of the latency between sending a want
// and receiving the corresponding block
type latencyTracker struct {
	alpha    float64
	samples  int
	smoothed float64
}

func newLatencyTracker(alpha float64) *latencyTracker {
	return &latencyTracker{alpha: alpha}
}

func (lt *latencyTracker) hasLatency() bool {
	return lt.samples > 0
}

func (lt *latencyTracker) smoothedLatency() time.Duration {
	return time.Duration(lt.smoothed)
}

func (lt *latencyTracker) receiveUpdate(count int, totalLatency time.Duration) {
	lt.samples++

	// Initially set alpha to be 1.0 / <the number of samples>
	alpha := 1.0 / float64(lt.samples)
	if alpha < latencyTrackerAlpha {
		// Once we have enough samples, clamp alpha
		alpha = latencyTrackerAlpha
	}
	lt.smoothed = ewma(lt.smoothed, float64(totalLatency)/float64(count), alpha)
}

func ewma(old, new, alpha float64) float64 {
	return new*alpha + (1-alpha)*old
}
