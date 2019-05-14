package monitor

import (
	"context"
	"testing"
	"time"
)

func TestAdding(t *testing.T) {
	monitor := New(time.Duration(0))
	var start1Called bool
	var start2Called bool
	var start3Called bool
	var end1Called bool
	var end2Called bool
	var end3Called bool
	var waitForCompletion1Called bool
	var waitForCompletion2Called bool
	var waitForCompletion3Called bool

	startFunc1 := func() {
		start1Called = true
	}
	startFunc2 := func() {
		start2Called = true
	}
	startFunc3 := func() {
		start3Called = true
	}
	endFunc1 := func() {
		end1Called = true
	}
	endFunc2 := func() {
		end2Called = true
	}
	endFunc3 := func() {
		end3Called = true
	}
	closingChan1 := make(chan struct{})
	waitForCompletionFunc1 := func() {
		<-closingChan1
		waitForCompletion1Called = true
	}
	closingChan2 := make(chan struct{})
	waitForCompletionFunc2 := func() {
		<-closingChan2
		waitForCompletion2Called = true
	}
	closingChan3 := make(chan struct{})
	waitForCompletionFunc3 := func() {
		<-closingChan3
		waitForCompletion3Called = true
	}
	monitor.Add(startFunc1, endFunc1, waitForCompletionFunc1)
	monitor.Add(startFunc2, endFunc2, waitForCompletionFunc2)

	if start1Called || start2Called {
		t.Fatal("Start functions should not start till monitor is started")
	}
	monitor.Start()
	if !start1Called || !start2Called {
		t.Fatal("Start functions should start once monitor is started")
	}
	monitor.Add(startFunc3, endFunc3, waitForCompletionFunc3)
	if !start3Called {
		t.Fatal("Tasks added after start should be started immediately")
	}
	if end1Called || end2Called || end3Called {
		t.Fatal("Tasks should not be ended until shutdown is calls")
	}
	go monitor.Shutdown()

	<-monitor.Closing()
	if !end1Called || !end2Called || !end3Called {
		t.Fatal("Tasks should have been told to begin closing once shutdown is called")
	}
	if waitForCompletion1Called || waitForCompletion2Called || waitForCompletion3Called {
		t.Fatal("Completion should not have happened until channel is closed")
	}
	close(closingChan1)
	close(closingChan2)
	close(closingChan3)
	<-monitor.Closed()
	if !waitForCompletion1Called || !waitForCompletion2Called || !waitForCompletion3Called {
		t.Fatal("Completion should have happened once channel is closed")
	}
}

func TestShutdownTimeout(t *testing.T) {
	startFunc := func() {}
	endFunc := func() {}
	closingChan := make(chan struct{})
	waitForCompletionFunc := func() {
		<-closingChan
	}

	monitor := New(10 * time.Millisecond)
	monitor.Add(startFunc, endFunc, waitForCompletionFunc)

	monitor.Start()

	go monitor.Shutdown()

	select {
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown should have timed out but did not")
	case <-monitor.Closed():
		close(closingChan)
	}
}

func TestAddCancellable(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	monitor := New(time.Duration(0))
	completed1 := make(chan struct{})
	monitor.AddCancellable(ctx, func(subCtx context.Context) {
		<-subCtx.Done()
		close(completed1)
	})
	completed2 := make(chan struct{})
	subCtx2, subCancel := context.WithCancel(ctx)
	monitor.AddCancellable(subCtx2, func(subCtx context.Context) {
		<-subCtx.Done()
		close(completed2)
	})
	monitor.Start()
	// can cancel via context
	subCancel()
	select {
	case <-ctx.Done():
		t.Fatal("Should have cancelled monitored process but did not")
	case <-completed2:
	}

	// can cancel via shutdown
	monitor.Shutdown()
	select {
	case <-ctx.Done():
		t.Fatal("Should have cancelled monitored process but did not")
	case <-completed1:
	}
}

func TestRunnable(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	completed := make(chan struct{})
	interrupted := make(chan struct{})
	started := make(chan struct{})
	monitor := New(time.Duration(0))
	monitor.AddRunnable(func() {
		close(started)
		<-interrupted
		close(completed)
	}, func() { close(interrupted) })
	monitor.Start()

	select {
	case <-ctx.Done():
		t.Fatal("runnable function should have been started but was not")
	case <-started:
	}

	select {
	case <-interrupted:
		t.Fatal("process should not be interrupted")
	case <-completed:
		t.Fatal("process should not be completed")
	default:
	}

	monitor.Shutdown()
	select {
	case <-ctx.Done():
		t.Fatal("runnable function should have been completed but was not")
	case <-completed:
	}
}

func TestLinkWithCancellableContext(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	monitor := New(time.Duration(0))
	subCtx1, subCancel1 := context.WithCancel(ctx)
	monitor.LinkContextCancellation(subCtx1, subCancel1)
	subCtx2, subCancel2 := context.WithCancel(context.Background())
	defer subCancel2()
	monitor.LinkContextCancellation(subCtx2, subCancel2)
	subCancel1()
	monitor.Start()

	select {
	case <-ctx.Done():
		t.Fatal("Process should have closed from linked context cancellation but did not")
	case <-monitor.Closed():
	}

	select {
	case <-ctx.Done():
		t.Fatal("linked context should have cancelled from process closing but didn't")
	case <-subCtx2.Done():
	}
}
