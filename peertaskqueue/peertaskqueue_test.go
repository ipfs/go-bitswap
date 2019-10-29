package peertaskqueue

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ipfs/go-bitswap/peertaskqueue/peertask"
	"github.com/ipfs/go-bitswap/peertaskqueue/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestPushPop(t *testing.T) {
	ptq := New()
	partner := testutil.GeneratePeers(1)[0]
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")
	consonants := func() []string {
		var out []string
		for _, letter := range alphabet {
			skip := false
			for _, vowel := range vowels {
				if letter == vowel {
					skip = true
				}
			}
			if !skip {
				out = append(out, letter)
			}
		}
		return out
	}()
	sort.Strings(alphabet)
	sort.Strings(vowels)
	sort.Strings(consonants)

	// add a bunch of blocks. cancel some. drain the queue. the queue should only have the kept tasks

	for _, index := range rand.Perm(len(alphabet)) { // add blocks for all letters
		letter := alphabet[index]
		t.Log(letter)

		// add tasks out of order, but with in-order priority
		ptq.PushTasks(partner, peertask.Task{Identifier: letter, Priority: math.MaxInt32 - index})
	}
	for _, consonant := range consonants {
		ptq.Remove(consonant, partner)
	}

	ptq.FullThaw()

	var out []string
	for {
		_, received := ptq.PopTasks("", 100)
		if len(received) == 0 {
			break
		}

		for _, task := range received {
			out = append(out, task.Identifier.(string))
		}
	}

	// Tasks popped should already be in correct order
	for i, expected := range vowels {
		if out[i] != expected {
			t.Fatal("received", out[i], "expected", expected)
		}
	}
}

func TestFreezeUnfreeze(t *testing.T) {
	ptq := New()
	peers := testutil.GeneratePeers(4)
	a := peers[0]
	b := peers[1]
	c := peers[2]
	d := peers[3]

	// Push 5 blocks to each peer
	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		ptq.PushTasks(a, peertask.Task{Identifier: is, EntrySize: 1})
		ptq.PushTasks(b, peertask.Task{Identifier: is, EntrySize: 1})
		ptq.PushTasks(c, peertask.Task{Identifier: is, EntrySize: 1})
		ptq.PushTasks(d, peertask.Task{Identifier: is, EntrySize: 1})
	}

	// now, pop off four tasks, there should be one from each
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

	ptq.Remove("1", b)

	// b should be frozen, causing it to get skipped in the rotation
	matchNTasks(t, ptq, 3, a.Pretty(), c.Pretty(), d.Pretty())

	ptq.ThawRound()

	matchNTasks(t, ptq, 1, b.Pretty())

	// remove none existent task
	ptq.Remove("-1", b)

	// b should not be frozen
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

}

func TestFreezeUnfreezeNoFreezingOption(t *testing.T) {
	ptq := New(IgnoreFreezing(true))
	peers := testutil.GeneratePeers(4)
	a := peers[0]
	b := peers[1]
	c := peers[2]
	d := peers[3]

	// Have each push some blocks

	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		ptq.PushTasks(a, peertask.Task{Identifier: is, EntrySize: 1})
		ptq.PushTasks(b, peertask.Task{Identifier: is, EntrySize: 1})
		ptq.PushTasks(c, peertask.Task{Identifier: is, EntrySize: 1})
		ptq.PushTasks(d, peertask.Task{Identifier: is, EntrySize: 1})
	}

	// now, pop off four tasks, there should be one from each
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())

	ptq.Remove("1", b)

	// b should not be frozen, so it wont get skipped in the rotation
	matchNTasks(t, ptq, 4, a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty())
}

// This test checks that ordering of peers is correct
func TestPeerOrder(t *testing.T) {
	ptq := New()
	peers := testutil.GeneratePeers(3)
	a := peers[0]
	b := peers[1]
	c := peers[2]

	ptq.PushTasks(a, peertask.Task{Identifier: "1", EntrySize: 3, Priority: 3})
	ptq.PushTasks(a, peertask.Task{Identifier: "2", EntrySize: 1, Priority: 2})
	ptq.PushTasks(a, peertask.Task{Identifier: "3", EntrySize: 2, Priority: 1})

	ptq.PushTasks(b, peertask.Task{Identifier: "4", EntrySize: 1, Priority: 3})
	ptq.PushTasks(b, peertask.Task{Identifier: "5", EntrySize: 3, Priority: 2})
	ptq.PushTasks(b, peertask.Task{Identifier: "6", EntrySize: 1, Priority: 1})

	ptq.PushTasks(c, peertask.Task{Identifier: "7", EntrySize: 2, Priority: 3})
	ptq.PushTasks(c, peertask.Task{Identifier: "8", EntrySize: 2, Priority: 1})

	// All peers have nothing in their active queue, so equal chance of any
	// peer being chosen
	var ps []string
	var ids []string
	for i := 0; i < 3; i++ {
		p, tasks := ptq.PopTasks("", 3)
		ps = append(ps, p.String())
		ids = append(ids, fmt.Sprint(tasks[0].Identifier))
	}
	matchArrays(t, ps, []string{a.String(), b.String(), c.String()})
	matchArrays(t, ids, []string{"1", "4", "7"})

	// Active queues:
	// a: 3            Pending: [1, 2]
	// b: 1            Pending: [3, 1]
	// c: 2            Pending: [2]
	// So next peer should be b
	p, tsk := ptq.PopTasks("", 3)
	if len(tsk) != 1 || p != b || tsk[0].Identifier != "5" {
		t.Fatal("Expected ID 5 from peer b")
	}

	// Active queues:
	// a: 3            Pending: [1, 2]
	// b: 1 + 3        Pending: [1]
	// c: 2            Pending: [2]
	// So next peer should be c
	p, tsk = ptq.PopTasks("", 3)
	if len(tsk) != 1 || p != c || tsk[0].Identifier != "8" {
		t.Fatal("Expected ID 8 from peer c")
	}

	// Active queues:
	// a: 3            Pending: [1, 2]
	// b: 1 + 3        Pending: [1]
	// c: 2 + 2
	// So next peer should be a
	p, tsk = ptq.PopTasks("", 3)
	if len(tsk) != 2 || p != a || tsk[0].Identifier != "2" || tsk[1].Identifier != "3" {
		t.Fatal("Expected ID 2 & 3 from peer a")
	}

	// Active queues:
	// a: 3 + 1 + 2
	// b: 1 + 3        Pending: [1]
	// c: 2 + 2
	// a & c have no more pending tasks, so next peer should be b
	p, tsk = ptq.PopTasks("", 3)
	if len(tsk) != 1 || p != b || tsk[0].Identifier != "6" {
		t.Fatal("Expected ID 6 from peer b")
	}

	// Active queues:
	// a: 3 + 1 + 2
	// b: 1 + 3 + 1
	// c: 2 + 2
	// No more pending tasks, so next pop should return nothing
	_, tsk = ptq.PopTasks("", 3)
	if len(tsk) != 0 {
		t.Fatal("Expected no more tasks")
	}
}

// This test checks that we can pop multiple times from the same peer
func TestPopSamePeer(t *testing.T) {
	ptq := New()
	peers := testutil.GeneratePeers(2)
	a := peers[0]
	b := peers[1]

	ptq.PushTasks(a, peertask.Task{Identifier: "1", EntrySize: 3, Priority: 2})
	ptq.PushTasks(a, peertask.Task{Identifier: "2", EntrySize: 1, Priority: 1})

	ptq.PushTasks(b, peertask.Task{Identifier: "3", EntrySize: 1, Priority: 2})
	ptq.PushTasks(b, peertask.Task{Identifier: "4", EntrySize: 3, Priority: 1})

	// Neither peers has anything in its active queue, so equal chance of any
	// peer being chosen
	var ps []string
	var ids []string
	for i := 0; i < 2; i++ {
		p, tasks := ptq.PopTasks("", 3)
		ps = append(ps, p.String())
		ids = append(ids, fmt.Sprint(tasks[0].Identifier))
	}
	matchArrays(t, ps, []string{a.String(), b.String()})
	matchArrays(t, ids, []string{"1", "3"})

	// Active queues:
	// a: 3            Pending: [1]
	// b: 1            Pending: [3]
	// Peer b has smallest active byte size in its queue, so it would be chosen
	// but we explicitly request peer a
	p, tsk := ptq.PopTasks(a, 3)
	if len(tsk) != 1 || p != a || tsk[0].Identifier != "2" {
		t.Fatal("Expected ID 2 from peer a", tsk)
	}

	// Active queues:
	// a: 3 + 1
	// b: 1            Pending: [3]
	// Peer b has smallest active byte size in its queue, so it would be chosen
	// but we explicitly request peer a again
	_, tsk = ptq.PopTasks(a, 3)
	if len(tsk) != 0 {
		t.Fatal("Expected no tasks (request for peer a tasks)")
	}

	// Active queues:
	// a: 3 + 1
	// b: 1            Pending: [3]
	// Now we dont request a specific peer, expect peer b
	p, tsk = ptq.PopTasks("", 3)
	if len(tsk) != 1 || p != b || tsk[0].Identifier != "4" {
		t.Fatal("Expected ID 4 from peer b")
	}

	// Active queues:
	// a: 3 + 1
	// b: 1 + 3
	// No more pending tasks, so next pop should return nothing
	_, tsk = ptq.PopTasks("", 3)
	if len(tsk) != 0 {
		t.Fatal("Expected no more tasks")
	}
}

func TestHooks(t *testing.T) {
	var peersAdded []string
	var peersRemoved []string
	onPeerAdded := func(p peer.ID) {
		peersAdded = append(peersAdded, p.Pretty())
	}
	onPeerRemoved := func(p peer.ID) {
		peersRemoved = append(peersRemoved, p.Pretty())
	}
	ptq := New(OnPeerAddedHook(onPeerAdded), OnPeerRemovedHook(onPeerRemoved))
	peers := testutil.GeneratePeers(2)
	a := peers[0]
	b := peers[1]
	ptq.PushTasks(a, peertask.Task{Identifier: "1"})
	ptq.PushTasks(b, peertask.Task{Identifier: "2"})
	expected := []string{a.Pretty(), b.Pretty()}
	sort.Strings(expected)
	sort.Strings(peersAdded)
	if len(peersAdded) != len(expected) {
		t.Fatal("Incorrect number of peers added")
	}
	for i, s := range peersAdded {
		if expected[i] != s {
			t.Fatal("unexpected peer", s, expected[i])
		}
	}

	p, task := ptq.PopTasks("", 100)
	ptq.TasksDone(p, task...)
	p, task = ptq.PopTasks("", 100)
	ptq.TasksDone(p, task...)
	ptq.PopTasks("", 100)
	ptq.PopTasks("", 100)

	sort.Strings(peersRemoved)
	if len(peersRemoved) != len(expected) {
		t.Fatal("Incorrect number of peers removed")
	}
	for i, s := range peersRemoved {
		if expected[i] != s {
			t.Fatal("unexpected peer", s, expected[i])
		}
	}
}
func TestCleaningUpQueues(t *testing.T) {
	ptq := New()

	peer := testutil.GeneratePeers(1)[0]
	var peerTasks []peertask.Task
	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		peerTasks = append(peerTasks, peertask.Task{Identifier: is})
	}

	// push a block, pop a block, complete everything, should be removed
	ptq.PushTasks(peer, peerTasks...)
	p, task := ptq.PopTasks("", 100)
	ptq.TasksDone(p, task...)
	_, task = ptq.PopTasks("", 100)

	if len(task) != 0 || len(ptq.peerTrackers) > 0 || ptq.pQueue.Len() > 0 {
		t.Fatal("PeerTracker should have been removed because it's idle")
	}

	// push a block, remove each of its entries, should be removed
	ptq.PushTasks(peer, peerTasks...)
	for _, peerTask := range peerTasks {
		ptq.Remove(peerTask.Identifier, peer)
	}
	_, task = ptq.PopTasks("", 100)

	if len(task) != 0 || len(ptq.peerTrackers) > 0 || ptq.pQueue.Len() > 0 {
		t.Fatal("Partner should have been removed because it's idle")
	}

}

func matchNTasks(t *testing.T, ptq *PeerTaskQueue, n int, expected ...string) []peertask.Task {
	var targets []string
	var tasks []peertask.Task
	for i := 0; i < n; i++ {
		p, tsk := ptq.PopTasks("", 1)
		if len(tsk) != 1 {
			t.Fatal("expected 1 task at a time")
		}
		targets = append(targets, p.Pretty())
		tasks = append(tasks, tsk...)
	}

	matchArrays(t, expected, targets)
	return tasks
}

func matchArrays(t *testing.T, str1, str2 []string) {
	if len(str1) != len(str2) {
		t.Fatal("array lengths did not match", str1, str2)
	}

	sort.Strings(str1)
	sort.Strings(str2)

	t.Log(str1)
	t.Log(str2)
	for i, s := range str2 {
		if str1[i] != s {
			t.Fatal("unexpected peer", s, str1[i])
		}
	}
}
