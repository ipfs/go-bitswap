package peertaskqueue

import (
	"sync"

	"github.com/ipfs/go-bitswap/peertaskqueue/peertask"
	"github.com/ipfs/go-bitswap/peertaskqueue/peertracker"
	pq "github.com/ipfs/go-ipfs-pq"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type peerTaskQueueEvent int

const (
	peerAdded   = peerTaskQueueEvent(1)
	peerRemoved = peerTaskQueueEvent(2)
)

type hookFunc func(p peer.ID, event peerTaskQueueEvent)

// PeerTaskQueue is a prioritized list of tasks to be executed on peers.
// Tasks are added to the queue, then popped off alternately between peers (roughly)
// to execute the block with the highest priority, or otherwise the one added
// first if priorities are equal.
type PeerTaskQueue struct {
	lock           sync.Mutex
	pQueue         pq.PQ
	peerTrackers   map[peer.ID]*peertracker.PeerTracker
	frozenPeers    map[peer.ID]struct{}
	hooks          []hookFunc
	ignoreFreezing bool
}

// Option is a function that configures the peer task queue
type Option func(*PeerTaskQueue) Option

func chain(firstOption Option, secondOption Option) Option {
	return func(ptq *PeerTaskQueue) Option {
		firstReverse := firstOption(ptq)
		secondReverse := secondOption(ptq)
		return chain(secondReverse, firstReverse)
	}
}

// IgnoreFreezing is an option that can make the task queue ignore freezing and unfreezing
func IgnoreFreezing(ignoreFreezing bool) Option {
	return func(ptq *PeerTaskQueue) Option {
		previous := ptq.ignoreFreezing
		ptq.ignoreFreezing = ignoreFreezing
		return IgnoreFreezing(previous)
	}
}

func removeHook(hook hookFunc) Option {
	return func(ptq *PeerTaskQueue) Option {
		for i, testHook := range ptq.hooks {
			if &hook == &testHook {
				ptq.hooks = append(ptq.hooks[:i], ptq.hooks[i+1:]...)
				break
			}
		}
		return addHook(hook)
	}
}

func addHook(hook hookFunc) Option {
	return func(ptq *PeerTaskQueue) Option {
		ptq.hooks = append(ptq.hooks, hook)
		return removeHook(hook)
	}
}

// OnPeerAddedHook adds a hook function that gets called whenever the ptq adds a new peer
func OnPeerAddedHook(onPeerAddedHook func(p peer.ID)) Option {
	hook := func(p peer.ID, event peerTaskQueueEvent) {
		if event == peerAdded {
			onPeerAddedHook(p)
		}
	}
	return addHook(hook)
}

// OnPeerRemovedHook adds a hook function that gets called whenever the ptq adds a new peer
func OnPeerRemovedHook(onPeerRemovedHook func(p peer.ID)) Option {
	hook := func(p peer.ID, event peerTaskQueueEvent) {
		if event == peerRemoved {
			onPeerRemovedHook(p)
		}
	}
	return addHook(hook)
}

// New creates a new PeerTaskQueue
func New(options ...Option) *PeerTaskQueue {
	ptq := &PeerTaskQueue{
		peerTrackers: make(map[peer.ID]*peertracker.PeerTracker),
		frozenPeers:  make(map[peer.ID]struct{}),
		pQueue:       pq.New(peertracker.PeerCompare),
	}
	ptq.Options(options...)
	return ptq
}

// Options uses configuration functions to configure the peer task queue.
// It returns an Option that can be called to reverse the changes.
func (ptq *PeerTaskQueue) Options(options ...Option) Option {
	if len(options) == 0 {
		return nil
	}
	if len(options) == 1 {
		return options[0](ptq)
	}
	reverse := options[0](ptq)
	return chain(ptq.Options(options[1:]...), reverse)
}

func (ptq *PeerTaskQueue) callHooks(to peer.ID, event peerTaskQueueEvent) {
	for _, hook := range ptq.hooks {
		hook(to, event)
	}
}

// PushTasks adds a new group of tasks for the given peer to the queue
func (ptq *PeerTaskQueue) PushTasks(to peer.ID, tasks ...peertask.Task) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	peerTracker, ok := ptq.peerTrackers[to]
	if !ok {
		peerTracker = peertracker.New(to)
		ptq.pQueue.Push(peerTracker)
		ptq.peerTrackers[to] = peerTracker
		ptq.callHooks(to, peerAdded)
	}

	peerTracker.PushTasks(tasks)
	ptq.pQueue.Update(peerTracker.Index())
}

// PopTasks pops the highest priority tasks from the peer off the queue, up to
// the given maximum size of those tasks.
// If peer is "", the highest priority peer is chosen.
func (ptq *PeerTaskQueue) PopTasks(from peer.ID, maxSize int) (peer.ID, []peertask.Task) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	if ptq.pQueue.Len() == 0 {
		return "", nil
	}

	var peerTracker *peertracker.PeerTracker

	// If the peer wasn't specified, choose the highest priority peer
	popTracker := from == ""
	if popTracker {
		peerTracker = ptq.pQueue.Pop().(*peertracker.PeerTracker)
		if peerTracker == nil {
			return "", nil
		}
	} else {
		// Get the requested peer tracker
		var ok bool
		peerTracker, ok = ptq.peerTrackers[from]
		if !ok || peerTracker.IsIdle() {
			return "", nil
		}
	}

	// Get the highest priority tasks for the given peer
	out := peerTracker.PopTasks(maxSize)

	// If the peer has no more tasks, remove its peer tracker
	if peerTracker.IsIdle() {
		target := peerTracker.Target()
		delete(ptq.peerTrackers, target)
		delete(ptq.frozenPeers, target)
		ptq.callHooks(target, peerRemoved)
	} else if popTracker {
		// If it does have more tasks, and we popped it, put it back into the
		// peer queue
		ptq.pQueue.Push(peerTracker)
	}

	return peerTracker.Target(), out
}

// TasksDone is called to indicate that the given tasks have completed
// for the given peer
func (ptq *PeerTaskQueue) TasksDone(to peer.ID, tasks ...peertask.Task) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	// Get the peer tracker for the peer
	peerTracker, ok := ptq.peerTrackers[to]
	if !ok {
		return
	}

	// Tell the peer tracker that the tasks have completed
	for _, task := range tasks {
		peerTracker.TaskDone(task)
	}

	// This may affect the peer's position in the peer queue, so update if
	// necessary
	ptq.pQueue.Update(peerTracker.Index())
}

// Remove removes a task from the queue.
func (ptq *PeerTaskQueue) Remove(identifier peertask.Identifier, p peer.ID) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	peerTracker, ok := ptq.peerTrackers[p]
	if ok {
		if peerTracker.Remove(identifier) {
			// we now also 'freeze' that partner. If they sent us a cancel for a
			// block we were about to send them, we should wait a short period of time
			// to make sure we receive any other in-flight cancels before sending
			// them a block they already potentially have
			if !ptq.ignoreFreezing {
				if !peerTracker.IsFrozen() {
					ptq.frozenPeers[p] = struct{}{}
				}

				peerTracker.Freeze()
			}
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}

// FullThaw completely thaws all peers in the queue so they can execute tasks.
func (ptq *PeerTaskQueue) FullThaw() {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	for p := range ptq.frozenPeers {
		peerTracker, ok := ptq.peerTrackers[p]
		if ok {
			peerTracker.FullThaw()
			delete(ptq.frozenPeers, p)
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}

// ThawRound unthaws peers incrementally, so that those have been frozen the least
// become unfrozen and able to execute tasks first.
func (ptq *PeerTaskQueue) ThawRound() {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	for p := range ptq.frozenPeers {
		peerTracker, ok := ptq.peerTrackers[p]
		if ok {
			if peerTracker.Thaw() {
				delete(ptq.frozenPeers, p)
			}
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}
