package messagequeue

import (
	"time"
)

type MQDebouncer struct {
	scheduledAt  time.Time
	scheduleWork *time.Timer
	readyCh      chan struct{}
}

func newMQDebouncer() *MQDebouncer {
	mqd := &MQDebouncer{
		scheduleWork: time.NewTimer(0),
		readyCh:      make(chan struct{}),
	}
	mqd.stopTimer()
	return mqd
}

// The channel returned by ready emits when the debounce time is up.
func (mqd *MQDebouncer) ready() chan struct{} {
	go func() {
		<-mqd.scheduleWork.C
		mqd.scheduledAt = time.Time{}
		mqd.readyCh <- struct{}{}
	}()
	return mqd.readyCh
}

// Tells the MQDebouncer that a task was scheduled at the given time.
// Returns true if the maximum allowed time has expired, false otherwise.
func (mqd *MQDebouncer) scheduled(when time.Time) bool {
	// If we have work scheduled, cancel the timer. If we
	// don't, record when the work was scheduled.
	if mqd.scheduledAt.IsZero() {
		mqd.scheduledAt = when
	} else {
		mqd.stopTimer()
	}

	// Check if we've waited more than the maximum allowed debounce time
	if time.Since(mqd.scheduledAt) >= sendMessageMaxDelay {
		mqd.scheduledAt = time.Time{}
		return true
	}

	// Otherwise, extend the timer.
	mqd.scheduleWork.Reset(sendMessageDebounce)
	return false
}

func (mqd *MQDebouncer) stopTimer() {
	// Need to drain the timer if Stop() returns false
	// See: https://golang.org/pkg/time/#Timer.Stop
	if !mqd.scheduleWork.Stop() {
		<-mqd.scheduleWork.C
	}
}
