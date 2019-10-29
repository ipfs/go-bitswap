package peertracker

import (
	"sync"
	"time"

	"github.com/ipfs/go-bitswap/peertaskqueue/peertask"
	pq "github.com/ipfs/go-ipfs-pq"
	peer "github.com/libp2p/go-libp2p-core/peer"

	"github.com/google/uuid"
)

// PeerTracker tracks task blocks for a single peer, as well as active tasks
// for that peer
type PeerTracker struct {
	target peer.ID

	// Tasks that are pending being made active
	pendingTasks map[peertask.Identifier]*peertask.QueueTask
	// Tasks that have been made active
	activeTasks map[uuid.UUID]*peertask.QueueTask

	// activeBytes must be locked around as it will be updated externally
	activelk    sync.Mutex
	activeBytes int

	// for the PQ interface
	index int

	freezeVal int

	// priority queue of tasks belonging to this peer
	taskQueue pq.PQ
}

// New creates a new PeerTracker
func New(target peer.ID) *PeerTracker {
	return &PeerTracker{
		target:       target,
		taskQueue:    pq.New(peertask.WrapCompare(peertask.PriorityCompare)),
		pendingTasks: make(map[peertask.Identifier]*peertask.QueueTask),
		activeTasks:  make(map[uuid.UUID]*peertask.QueueTask),
	}
}

// PeerCompare implements pq.ElemComparator
// returns true if peer 'a' has higher priority than peer 'b'
func PeerCompare(a, b pq.Elem) bool {
	pa := a.(*PeerTracker)
	pb := b.(*PeerTracker)

	// having no pending tasks means lowest priority
	paPending := len(pa.pendingTasks)
	pbPending := len(pb.pendingTasks)
	if paPending == 0 {
		return false
	}
	if pbPending == 0 {
		return true
	}

	// Frozen peers have lowest priority
	if pa.freezeVal > pb.freezeVal {
		return false
	}
	if pa.freezeVal < pb.freezeVal {
		return true
	}

	// If each peer has an equal amount of active data in its queue, choose the
	// peer with the most amount of data waiting to send out
	if pa.activeBytes == pb.activeBytes {
		return paPending > pbPending
	}

	// Choose the peer with the least amount of active data in its queue.
	// This way we "keep peers busy" by sending them as much data as they can
	// process.
	return pa.activeBytes < pb.activeBytes
}

// Target returns the peer that this peer tracker tracks tasks for
func (p *PeerTracker) Target() peer.ID {
	return p.target
}

// IsIdle returns true if the peer has no active tasks or queued tasks
func (p *PeerTracker) IsIdle() bool {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	return len(p.pendingTasks) == 0 && len(p.activeTasks) == 0
}

// Index implements pq.Elem.
func (p *PeerTracker) Index() int {
	return p.index
}

// SetIndex implements pq.Elem.
func (p *PeerTracker) SetIndex(i int) {
	p.index = i
}

// PushTasks adds a group of tasks onto a peer's queue
func (p *PeerTracker) PushTasks(tasks []peertask.Task) {
	now := time.Now()

	p.activelk.Lock()
	defer p.activelk.Unlock()

	for _, task := range tasks {
		// If the new task doesn't add any more information over what we
		// already have in the active queue, then we can skip the new task
		if !p.taskHasMoreInfoThanActiveTasks(task) {
			continue
		}

		// If there is already a non-active task with this Identifier
		if existingTask, ok := p.pendingTasks[task.Identifier]; ok {
			// If the new task has a higher priority than the old task,
			if task.Priority > existingTask.Priority {
				// Update the priority and the task's position in the queue
				existingTask.Priority = task.Priority
				p.taskQueue.Update(existingTask.Index())
			}

			// If we now have block size information, update the task with
			// the new block size
			if !existingTask.HaveBlock && task.HaveBlock {
				existingTask.HaveBlock = task.HaveBlock
				existingTask.BlockSize = task.BlockSize
			}

			// If replacing a want-have with a want-block
			if !existingTask.IsWantBlock && task.IsWantBlock {
				// Change the type from want-have to want-block
				existingTask.IsWantBlock = true
				// If the want-have was a DONT_HAVE, or the want-block has a size
				if !existingTask.HaveBlock || task.HaveBlock {
					// Update the entry size
					existingTask.HaveBlock = task.HaveBlock
					existingTask.EntrySize = task.EntrySize
				}
			}

			// If the task is a want-block, make sure the entry size is equal
			// to the block size (because we will send the whole block)
			if existingTask.IsWantBlock && existingTask.HaveBlock {
				existingTask.EntrySize = existingTask.BlockSize
			}

			// A task with the Identifier exists, so we don't need to add
			// the new task to the queue
			continue
		}

		// Push the new task onto the queue
		qTask := peertask.NewQueueTask(task, p.target, now)
		p.pendingTasks[task.Identifier] = qTask
		p.taskQueue.Push(qTask)
	}
}

// PopTasks pops as many tasks as possible up to the given size off the queue
// in priority order
func (p *PeerTracker) PopTasks(maxSize int) []peertask.Task {
	var out []peertask.Task
	size := 0
	for p.taskQueue.Len() > 0 && p.freezeVal == 0 {
		// Pop a task off the queue
		t := p.taskQueue.Pop().(*peertask.QueueTask)

		// Ignore tasks that have been cancelled
		task, ok := p.pendingTasks[t.Identifier]
		if !ok {
			continue
		}

		// If the next task is too big for the message
		if size+task.EntrySize > maxSize {
			// This task doesn't fit into the message, so push it back onto the
			// queue.
			p.taskQueue.Push(task)

			// We have as many tasks as we can fit into the message, so return
			// the tasks
			return out
		}

		// Start the task (this makes it "active")
		p.startTask(task)

		out = append(out, task.Task)
		size = size + task.EntrySize
	}

	return out
}

// startTask signals that a task was started for this peer.
func (p *PeerTracker) startTask(task *peertask.QueueTask) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	// Remove task from pending queue
	delete(p.pendingTasks, task.Identifier)

	// Add task to active queue
	if _, ok := p.activeTasks[task.Uuid]; !ok {
		p.activeTasks[task.Uuid] = task
		p.activeBytes += task.EntrySize
	}
}

// TaskDone signals that a task was completed for this peer.
func (p *PeerTracker) TaskDone(task peertask.Task) {
	p.activelk.Lock()
	defer p.activelk.Unlock()

	// Remove task from active queue
	if task, ok := p.activeTasks[task.Uuid]; ok {
		delete(p.activeTasks, task.Uuid)
		p.activeBytes -= task.EntrySize
		if p.activeBytes < 0 {
			panic("more tasks finished than started!")
		}
	}
}

// Remove removes the task with the given identifier from this peer's queue
func (p *PeerTracker) Remove(identifier peertask.Identifier) bool {
	return p.remove(identifier, true) || p.remove(identifier, false)
}

func (p *PeerTracker) remove(identifier peertask.Identifier, isWantBlock bool) bool {
	_, ok := p.pendingTasks[identifier]
	if ok {
		delete(p.pendingTasks, identifier)
	}
	return ok
}

// Freeze increments the freeze value for this peer. While a peer is frozen
// (freeze value > 0) it will not execute tasks.
func (p *PeerTracker) Freeze() {
	p.freezeVal++
}

// Thaw decrements the freeze value for this peer. While a peer is frozen
// (freeze value > 0) it will not execute tasks.
func (p *PeerTracker) Thaw() bool {
	p.freezeVal -= (p.freezeVal + 1) / 2
	return p.freezeVal <= 0
}

// FullThaw completely unfreezes this peer so it can execute tasks.
func (p *PeerTracker) FullThaw() {
	p.freezeVal = 0
}

// IsFrozen returns whether this peer is frozen and unable to execute tasks.
func (p *PeerTracker) IsFrozen() bool {
	return p.freezeVal > 0
}

// Indicates whether the new task adds any more information over tasks that are
// already in the active task queue
func (p *PeerTracker) taskHasMoreInfoThanActiveTasks(task peertask.Task) bool {
	taskWithIdExists := false
	haveSize := false
	haveBlock := false
	for _, at := range p.activeTasks {
		if task.Identifier == at.Identifier {
			taskWithIdExists = true

			if at.HaveBlock {
				haveSize = true
			}

			if at.IsWantBlock {
				haveBlock = true
			}
		}
	}

	// No existing task with that id, so the new task has more info
	if !taskWithIdExists {
		return true
	}

	// If there is no active want-block and the new task is a want-block,
	// the new task is better
	if !haveBlock && task.IsWantBlock {
		return true
	}

	// If there is no size information for the CID and the new task has
	// size information, the new task is better
	if !haveSize && task.HaveBlock {
		return true
	}

	return false
}
