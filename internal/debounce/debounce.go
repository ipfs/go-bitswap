package debounce

import (
	"sync"
	"time"
)

type Option func(d *debouncerSettings)

// The maximum time to wait before forcing invocation of the function
func MaxWait(maxWait time.Duration) Option {
	return func(d *debouncerSettings) {
		d.maxWait = maxWait
	}
}

type debouncer struct {
	fn    func()
	s     *debouncerSettings
	lk    sync.Mutex
	timer *debounceTimer
}

type debouncerSettings struct {
	wait    time.Duration
	maxWait time.Duration
}

// New creates a new debouncer function that will only call `fn` after `wait`
// time has passed since the last invocation of the debouncer function.
// If a maximum wait time is supplied the debouncer will call the function
// after `maxWait` even if `wait` time has not passed since the last
// invocation.
func New(fn func(), wait time.Duration, opts ...Option) func() {
	s := &debouncerSettings{
		wait:    wait,
		maxWait: time.Duration(0),
	}

	for _, o := range opts {
		o(s)
	}

	d := &debouncer{
		fn: fn,
		s:  s,
	}

	return d.call
}

func (d *debouncer) call() {
	d.lk.Lock()
	defer d.lk.Unlock()

	if d.timer == nil {
		d.timer = newDebounceTimer(d.s, d.onTimerExpired)
	}
	d.timer.Reset()
}

func (d *debouncer) onTimerExpired() {
	d.lk.Lock()
	defer d.lk.Unlock()

	d.timer = nil
	d.fn()
}

type debounceTimer struct {
	reset chan struct{}
}

func newDebounceTimer(s *debouncerSettings, done func()) *debounceTimer {
	dt := &debounceTimer{reset: make(chan struct{}, 1)}
	go dt.run(s, done)
	return dt
}

func (dt *debounceTimer) run(s *debouncerSettings, done func()) {
	waitTimer := time.NewTimer(s.wait)
	maxWaitTimer := time.NewTimer(s.maxWait)
	if s.maxWait == 0 {
		maxWaitTimer.Stop()
	}

	defer done()

	for {
		select {
		case <-dt.reset:
			waitTimer.Reset(s.wait)
		case <-waitTimer.C:
			maxWaitTimer.Stop()
			return
		case <-maxWaitTimer.C:
			waitTimer.Stop()
			return
		}
	}
}

func (dt *debounceTimer) Reset() {
	select {
	case dt.reset <- struct{}{}:
	default:
	}
}
