package monitor

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Monitor starts and monitors closing of tasks, providing mechanisms
// to wait for tasks to complete. It does not assume contexts as the default
// waiting mechanism, but can be linked to them
type Monitor struct {
	childrenLk      sync.RWMutex
	children        []childRoutine
	wg              sync.WaitGroup
	closeOnce       sync.Once
	shutdownTimeout time.Duration
	err             error
	started         chan struct{}
	closed          chan struct{}
	closing         chan struct{}
}

// New returns a new Monitor with the given timeout duration
func New(shutdownTimeout time.Duration) *Monitor {
	return &Monitor{
		closed:          make(chan struct{}),
		closing:         make(chan struct{}),
		started:         make(chan struct{}),
		shutdownTimeout: shutdownTimeout,
		children:        make([]childRoutine, 0),
	}
}

// Add sets up a task to monitor. It takes three parameters:
// - a function to start the task
// - a function to tell the task to stop
// - a function to wait for a task to fully complete after it's told to stop
// How the underlying task manages it's internal operations is essentially an
// unknown
func (p *Monitor) Add(startFunc func(), stopFunc func(), waitForComplete func()) {
	p.childrenLk.Lock()
	defer p.childrenLk.Unlock()
	p.children = append(p.children, childRoutine{startFunc, stopFunc, waitForComplete})
	select {
	case <-p.started:
		startFunc()
	default:
	}
}

// AddRunnable is a task that is expressed as a function called to start and
// execute a task to completion, and an interrupt function that can cause the task
// to terminate.
func (p *Monitor) AddRunnable(runnable func(), interrupt func()) {
	completeChan := make(chan struct{})
	start := func() {
		go func() {
			defer close(completeChan)
			runnable()
		}()
	}
	waitForComplete := func() {
		<-completeChan
	}
	p.Add(start, interrupt, waitForComplete)
}

// AddCancellable is for tasks that are functions that take a context and run until
// that context is cancelled.
func (p *Monitor) AddCancellable(ctx context.Context, cancellable func(context.Context)) {
	subCtx, subCancel := context.WithCancel(ctx)
	p.AddRunnable(func() {
		defer subCancel()
		cancellable(subCtx)
	}, subCancel)
}

// LinkContextCancellation links a monitor to a context, so shutdown the monitor
// cancels the context and cancelling the context shuts down the monitor
func (p *Monitor) LinkContextCancellation(ctx context.Context, cancel context.CancelFunc) {
	go func() {
		defer cancel()
		select {
		case <-p.closing:
		case <-ctx.Done():
			p.Shutdown()
		}
	}()
}

// Start initiates tasks given to the monitor. Prior to calling start, added tasks
// are not started. After calling start, added tasks start as soon as
// you add them
func (p *Monitor) Start() {
	p.childrenLk.RLock()
	defer p.childrenLk.RUnlock()
	for _, cr := range p.children {
		cr.start()
	}
	close(p.started)
}

// Shutdown closes a monitor and waits for its underlying tasks to complete or
// a timeout to be reached
func (p *Monitor) Shutdown() error {
	p.closeOnce.Do(func() {
		p.childrenLk.RLock()
		defer p.childrenLk.RUnlock()
		defer close(p.closed)
		for _, cr := range p.children {
			cr.stop()
		}
		close(p.closing)
		p.wg.Add(len(p.children))
		for _, cr := range p.children {
			go func(cr childRoutine) {
				cr.waitForComplete()
				p.wg.Done()
			}(cr)
		}
		completeChan := make(chan struct{})
		go func() {
			defer close(completeChan)
			p.wg.Wait()
		}()
		timeoutChan := func() <-chan time.Time {
			if p.shutdownTimeout == 0 {
				return nil
			}
			return time.After(p.shutdownTimeout)
		}
		select {
		case <-completeChan:
		case <-timeoutChan():
			p.err = errors.New("Timeout Exceeded")
		}
	})
	return p.err
}

// Closing is a channel that is readable once the monitor has begun closing
func (p *Monitor) Closing() <-chan struct{} {
	return p.closing
}

// Closed is a channel that is readable once the monitor is finished closing
func (p *Monitor) Closed() <-chan struct{} {
	return p.closed
}

// Err is the result of the Shutdown operation, or nil is not errors occurred
func (p *Monitor) Err() error {
	select {
	case <-p.closed:
		return p.err
	default:
		return nil
	}
}

type childRoutine struct {
	start           func()
	stop            func()
	waitForComplete func()
}
