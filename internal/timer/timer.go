package timer

import (
	"time"
)

func StopTimer(t *time.Timer) {
	if !t.Stop() {
		// Need to drain the timer if Stop() returns false
		// See: https://golang.org/pkg/time/#Timer.Stop
		<-t.C
	}
}
