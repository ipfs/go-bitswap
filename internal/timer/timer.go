package timer

import (
	"time"
)

// Stop the timer and drain the channel.
// Note that this should only be called if the channel hasn't already been
// drained or it may block indefinitely.
func StopTimer(t *time.Timer) {
	if !t.Stop() {
		// Need to drain the timer if Stop() returns false
		// See: https://golang.org/pkg/time/#Timer.Stop
		<-t.C
	}
}
