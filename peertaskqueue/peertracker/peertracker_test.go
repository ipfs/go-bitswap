package peertracker

import (
	"testing"

	"github.com/ipfs/go-bitswap/peertaskqueue/peertask"
	"github.com/ipfs/go-bitswap/peertaskqueue/testutil"
)

func TestEmpty(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	if len(tracker.PopTasks(100)) != 0 {
		t.Fatal("Expected no tasks")
	}
}

func TestPushPop(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     1,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "1" {
		t.Fatal("Expected same task")
	}
}

func TestPopNegativeOrZeroSize(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     1,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(-1)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
	popped = tracker.PopTasks(0)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
}

func TestPushPopSizeAndOrder(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     20,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "3",
			Priority:     15,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
	}
	tracker.PushTasks(tasks)
	popped := tracker.PopTasks(5)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}

	popped = tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "2" {
		t.Fatal("Expected tasks in order")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Identifier != "3" || popped[1].Identifier != "1" {
		t.Fatal("Expected tasks in order")
	}

	popped = tracker.PopTasks(100)
	if len(popped) != 0 {
		t.Fatal("Expected 0 tasks")
	}
}

func TestRemove(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     20,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "3",
			Priority:     15,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
	}
	tracker.PushTasks(tasks)
	tracker.Remove("2")
	popped := tracker.PopTasks(100)
	if len(popped) != 2 {
		t.Fatal("Expected 2 tasks")
	}
	if popped[0].Identifier != "3" || popped[1].Identifier != "1" {
		t.Fatal("Expected tasks in order")
	}
}

func TestRemoveMulti(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			BlockSize:    10,
			EntrySize:    10,
			HaveBlock:    true,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "1",
			Priority:     20,
			IsWantBlock:  false,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    1,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     15,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
	}
	tracker.PushTasks(tasks)
	tracker.Remove("1")
	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "2" {
		t.Fatal("Expected remaining task")
	}
}

func TestRemoveActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]
	tracker := New(partner)

	tasks := []peertask.Task{
		peertask.Task{
			Identifier:   "1",
			Priority:     10,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "1",
			Priority:     20,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    1,
			SendDontHave: false,
		},
		peertask.Task{
			Identifier:   "2",
			Priority:     15,
			IsWantBlock:  true,
			BlockSize:    10,
			HaveBlock:    true,
			EntrySize:    10,
			SendDontHave: false,
		},
	}

	tracker.PushTasks(tasks)

	// Pop highest priority task, ie ID "1" want-have
	// This makes the task active
	popped := tracker.PopTasks(10)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "1" {
		t.Fatal("Expected tasks in order")
	}

	// Remove all tasks with ID "1"
	tracker.Remove("1")
	popped = tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatal("Expected 1 task")
	}
	if popped[0].Identifier != "2" {
		t.Fatal("Expected tasks in order")
	}
}

func TestPushHaveVsBlock(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    1,
		SendDontHave: false,
	}
	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    10,
		SendDontHave: false,
	}

	runTestCase := func(tasks []peertask.Task, expIsWantBlock bool) {
		tracker := New(partner)
		tracker.PushTasks(tasks)
		popped := tracker.PopTasks(100)
		if len(popped) != 1 {
			t.Fatalf("Expected 1 task, received %d tasks", len(popped))
		}
		if popped[0].IsWantBlock != expIsWantBlock {
			t.Fatalf("Expected task.IsWantBlock to be %t, received %t", expIsWantBlock, popped[0].IsWantBlock)
		}
	}
	const wantBlockType = true
	const wantHaveType = false

	// should ignore second want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, wantHaveType)
	// should ignore second want-block
	runTestCase([]peertask.Task{wantBlock, wantBlock}, wantBlockType)
	// want-have does not overwrite want-block
	runTestCase([]peertask.Task{wantBlock, wantHave}, wantBlockType)
	// want-block overwrites want-have
	runTestCase([]peertask.Task{wantHave, wantBlock}, wantBlockType)
}

func TestPushSizeInfo(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    10,
		SendDontHave: false,
	}
	wantBlockDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    0,
		HaveBlock:    false,
		EntrySize:    2,
		SendDontHave: false,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    1,
		SendDontHave: false,
	}
	wantHaveDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    0,
		HaveBlock:    false,
		EntrySize:    1,
		SendDontHave: false,
	}

	runTestCase := func(tasks []peertask.Task, expEntrySize int, expBlockSize int, expIsWantBlock bool) {
		tracker := New(partner)
		tracker.PushTasks(tasks)
		popped := tracker.PopTasks(100)
		if len(popped) != 1 {
			t.Fatalf("Expected 1 task, received %d tasks", len(popped))
		}
		if popped[0].EntrySize != expEntrySize {
			t.Fatalf("Expected task.EntrySize to be %d, received %d", expEntrySize, popped[0].EntrySize)
		}
		if popped[0].BlockSize != expBlockSize {
			t.Fatalf("Expected task.EntrySize to be %d, received %d", expBlockSize, popped[0].BlockSize)
		}
		if popped[0].IsWantBlock != expIsWantBlock {
			t.Fatalf("Expected task.IsWantBlock to be %t, received %t", expIsWantBlock, popped[0].IsWantBlock)
		}
	}

	isWantBlock := true
	isWantHave := false

	// want-block (DONT_HAVE) should have no effect on existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlockDontHave}, wantBlockDontHave.EntrySize, wantBlockDontHave.BlockSize, isWantBlock)
	// want-have (DONT_HAVE) should have no effect on existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHaveDontHave}, wantBlockDontHave.EntrySize, wantBlockDontHave.BlockSize, isWantBlock)
	// want-block with size should update existing want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlock}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)
	// want-have with size should update existing want-block (DONT_HAVE) size,
	// but leave it as a want-block (ie should not change it to want-have)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHave}, wantHave.BlockSize, wantHave.BlockSize, isWantBlock)

	// want-block (DONT_HAVE) size should not update existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlockDontHave}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)
	// want-have (DONT_HAVE) should have no effect on existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHaveDontHave}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)
	// want-block with size should have no effect on existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlock}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)
	// want-have with size should have no effect on existing want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHave}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)

	// want-block (DONT_HAVE) should update type and entry size of existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlockDontHave}, wantBlockDontHave.EntrySize, wantBlockDontHave.BlockSize, isWantBlock)
	// want-have (DONT_HAVE) should have no effect on existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHaveDontHave}, wantHaveDontHave.EntrySize, wantHaveDontHave.BlockSize, isWantHave)
	// want-block with size should update existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlock}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)
	// want-have with size should update existing want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHave}, wantHave.EntrySize, wantHave.BlockSize, isWantHave)

	// want-block (DONT_HAVE) should update type and entry size of existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlockDontHave}, wantHave.BlockSize, wantHave.BlockSize, isWantBlock)
	// want-have (DONT_HAVE) should not update existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantHaveDontHave}, wantHave.EntrySize, wantHave.BlockSize, isWantHave)
	// want-block with size should update type and entry size of existing want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlock}, wantBlock.EntrySize, wantBlock.BlockSize, isWantBlock)
	// want-have should have no effect on existing want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, wantHave.EntrySize, wantHave.BlockSize, isWantHave)
}

func TestPushHaveVsBlockActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    10,
		SendDontHave: false,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    1,
		SendDontHave: false,
	}

	runTestCase := func(tasks []peertask.Task, expCount int) {
		tracker := New(partner)
		var popped []peertask.Task
		for _, task := range tasks {
			// Push the task
			tracker.PushTasks([]peertask.Task{task})
			// Pop the task (which makes it active)
			popped = append(popped, tracker.PopTasks(10)...)
		}
		if len(popped) != expCount {
			t.Fatalf("Expected %d tasks, received %d tasks", expCount, len(popped))
		}
	}

	// should ignore second want-have
	runTestCase([]peertask.Task{wantHave, wantHave}, 1)
	// should ignore second want-block
	runTestCase([]peertask.Task{wantBlock, wantBlock}, 1)
	// want-have does not overwrite want-block
	runTestCase([]peertask.Task{wantBlock, wantHave}, 1)
	// can't replace want-have with want-block because want-have is active
	runTestCase([]peertask.Task{wantHave, wantBlock}, 2)
}

func TestPushSizeInfoActive(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    10,
		SendDontHave: false,
	}
	wantBlockDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    0,
		HaveBlock:    false,
		EntrySize:    2,
		SendDontHave: false,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    1,
		SendDontHave: false,
	}
	wantHaveDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    0,
		HaveBlock:    false,
		EntrySize:    1,
		SendDontHave: false,
	}

	runTestCase := func(tasks []peertask.Task, expTasks []peertask.Task) {
		tracker := New(partner)
		var popped []peertask.Task
		for _, task := range tasks {
			// Push the task
			tracker.PushTasks([]peertask.Task{task})
			// Pop the task (which makes it active)
			popped = append(popped, tracker.PopTasks(100)...)
		}
		if len(popped) != len(expTasks) {
			t.Fatalf("Expected %d tasks, received %d tasks", len(expTasks), len(popped))
		}
		for i, task := range popped {
			if task.IsWantBlock != expTasks[i].IsWantBlock {
				t.Fatalf("Expected IsWantBlock to be %t, received %t", expTasks[i].IsWantBlock, task.IsWantBlock)
			}
			if task.EntrySize != expTasks[i].EntrySize {
				t.Fatalf("Expected Size to be %d, received %d", expTasks[i].EntrySize, task.EntrySize)
			}
		}
	}

	// second want-block (DONT_HAVE) should be ignored
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlockDontHave}, []peertask.Task{wantBlockDontHave})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHaveDontHave}, []peertask.Task{wantBlockDontHave})
	// want-block with size should be added if there is existing active want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantBlock}, []peertask.Task{wantBlockDontHave, wantBlock})
	// want-have with size should be added if there is existing active want-block (DONT_HAVE)
	runTestCase([]peertask.Task{wantBlockDontHave, wantHave}, []peertask.Task{wantBlockDontHave, wantHave})

	// want-block (DONT_HAVE) should be added if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlockDontHave}, []peertask.Task{wantHaveDontHave, wantBlockDontHave})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHaveDontHave}, []peertask.Task{wantHaveDontHave})
	// want-block with size should be added if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantBlock}, []peertask.Task{wantHaveDontHave, wantBlock})
	// want-have with size should be added if there is existing active want-have (DONT_HAVE)
	runTestCase([]peertask.Task{wantHaveDontHave, wantHave}, []peertask.Task{wantHaveDontHave, wantHave})

	// want-block (DONT_HAVE) should be ignored if there is existing active want-block with size
	runTestCase([]peertask.Task{wantBlock, wantBlockDontHave}, []peertask.Task{wantBlock})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHaveDontHave}, []peertask.Task{wantBlock})
	// second want-block with size should be ignored
	runTestCase([]peertask.Task{wantBlock, wantBlock}, []peertask.Task{wantBlock})
	// want-have with size should be ignored if there is existing active want-block with size
	runTestCase([]peertask.Task{wantBlock, wantHave}, []peertask.Task{wantBlock})

	// want-block (DONT_HAVE) should be added if there is existing active want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlockDontHave}, []peertask.Task{wantHave, wantBlockDontHave})
	// want-have (DONT_HAVE) should be ignored if there is existing active want-have with size
	runTestCase([]peertask.Task{wantHave, wantHaveDontHave}, []peertask.Task{wantHave})
	// second want-have with size should be ignored
	runTestCase([]peertask.Task{wantHave, wantHave}, []peertask.Task{wantHave})
	// want-block with size should be added if there is existing active want-have with size
	runTestCase([]peertask.Task{wantHave, wantBlock}, []peertask.Task{wantHave, wantBlock})
}

func TestReplaceTaskThatIsActiveAndPending(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    10,
		SendDontHave: false,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    1,
		SendDontHave: false,
	}
	wantHaveDontHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    0,
		HaveBlock:    false,
		EntrySize:    1,
		SendDontHave: false,
	}

	tracker := New(partner)

	// Push a want-have (DONT_HAVE)
	tracker.PushTasks([]peertask.Task{wantHaveDontHave})

	// Pop the want-have (DONT_HAVE) (which makes it active)
	_ = tracker.PopTasks(20)

	// Push a second want-have (with a size). Should be added to the pending
	// queue.
	tracker.PushTasks([]peertask.Task{wantHave})

	// Push a want-block (should replace the pending want-have)
	tracker.PushTasks([]peertask.Task{wantBlock})

	popped := tracker.PopTasks(100)
	if len(popped) != 1 {
		t.Fatalf("Expected 1 task to be popped, received %d tasks", len(popped))
	}
	if !popped[0].IsWantBlock {
		t.Fatalf("Expected task to be want-block")
	}
}

func TestTaskDone(t *testing.T) {
	partner := testutil.GeneratePeers(1)[0]

	wantBlock := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  true,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    10,
		SendDontHave: false,
	}
	wantHave := peertask.Task{
		Identifier:   "1",
		Priority:     10,
		IsWantBlock:  false,
		BlockSize:    10,
		HaveBlock:    true,
		EntrySize:    1,
		SendDontHave: false,
	}

	runTestCase := func(tasks []peertask.Task, expCount int) {
		tracker := New(partner)
		var popped []peertask.Task
		for _, task := range tasks {
			// Push the task
			tracker.PushTasks([]peertask.Task{task})
			// Pop the task (which makes it active)
			poppedTask := tracker.PopTasks(10)
			if len(poppedTask) > 0 {
				popped = append(popped, poppedTask...)
				// Complete the task (which makes it inactive)
				tracker.TaskDone(poppedTask[0])
			}
		}
		if len(popped) != expCount {
			t.Fatalf("Expected %d tasks, received %d tasks", expCount, len(popped))
		}
	}

	// should allow second want-have after first is complete
	runTestCase([]peertask.Task{wantHave, wantHave}, 2)
	// should allow second want-block after first is complete
	runTestCase([]peertask.Task{wantBlock, wantBlock}, 2)
	// should allow want-have after want-block is complete
	runTestCase([]peertask.Task{wantBlock, wantHave}, 2)
	// should allow want-block after want-have is complete
	runTestCase([]peertask.Task{wantHave, wantBlock}, 2)
}
