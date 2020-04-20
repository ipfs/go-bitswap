package notifications

import (
	"bytes"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
)

func TestDuplicates(t *testing.T) {
	b1 := blocks.NewBlock([]byte("1"))
	b2 := blocks.NewBlock([]byte("2"))

	n := New()
	s := n.NewSubscription()
	ch := s.Blocks()
	s.Add(b1.Cid(), b2.Cid())

	go n.Publish(b1)
	blockRecvd, ok := <-ch
	if !ok {
		t.Fail()
	}
	assertBlocksEqual(t, b1, blockRecvd)

	n.Publish(b1) // ignored duplicate

	go n.Publish(b2)
	blockRecvd, ok = <-ch
	if !ok {
		t.Fail()
	}
	assertBlocksEqual(t, b2, blockRecvd)
}

func TestPublishSubscribe(t *testing.T) {
	blockSent := blocks.NewBlock([]byte("Greetings from The Interval"))

	n := New()
	s := n.NewSubscription()
	s.Add(blockSent.Cid())

	go n.Publish(blockSent)
	blockRecvd, ok := <-s.Blocks()
	if !ok {
		t.Fail()
	}
	assertBlocksEqual(t, blockRecvd, blockSent)
}

func TestSubscribeMany(t *testing.T) {
	e1 := blocks.NewBlock([]byte("1"))
	e2 := blocks.NewBlock([]byte("2"))

	n := New()
	s := n.NewSubscription()
	ch := s.Blocks()
	s.Add(e1.Cid(), e2.Cid())

	go n.Publish(e1)
	r1, ok := <-ch
	if !ok {
		t.Fatal("didn't receive first expected block")
	}
	assertBlocksEqual(t, e1, r1)

	go n.Publish(e2)
	r2, ok := <-ch
	if !ok {
		t.Fatal("didn't receive second expected block")
	}
	assertBlocksEqual(t, e2, r2)
}

// TestDuplicateSubscribe tests a scenario where a given block
// would be requested twice at the same time.
func TestDuplicateSubscribe(t *testing.T) {
	e1 := blocks.NewBlock([]byte("1"))

	n := New()
	s1 := n.NewSubscription()
	ch1 := s1.Blocks()
	s2 := n.NewSubscription()
	ch2 := s2.Blocks()

	s1.Add(e1.Cid())
	s2.Add(e1.Cid())

	go n.Publish(e1)
	r1, ok := <-ch1
	if !ok {
		t.Fatal("didn't receive first expected block")
	}
	assertBlocksEqual(t, e1, r1)

	r2, ok := <-ch2
	if !ok {
		t.Fatal("didn't receive second expected block")
	}
	assertBlocksEqual(t, e1, r2)
}

func TestCloseBeforeUnsubscribe(t *testing.T) {
	e1 := blocks.NewBlock([]byte("1"))

	n := New()
	s := n.NewSubscription()
	ch := s.Blocks()
	s.Add(e1.Cid())

	s.Close()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should have been closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel should have been closed")
	}
}

func TestPublishAfterClose(t *testing.T) {
	e1 := blocks.NewBlock([]byte("1"))
	e2 := blocks.NewBlock([]byte("2"))

	n := New()
	s := n.NewSubscription()
	ch := s.Blocks()
	s.Add(e1.Cid(), e2.Cid())

	go n.Publish(e1)

	r1, ok := <-ch
	if !ok {
		t.Fatal("didn't receive first expected block")
	}
	assertBlocksEqual(t, e1, r1)

	s.Close()

	go n.Publish(e1)

	time.Sleep(10 * time.Millisecond)

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should have been closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel should have been closed")
	}
}

func assertBlocksEqual(t *testing.T, a, b blocks.Block) {
	if !bytes.Equal(a.RawData(), b.RawData()) {
		t.Fatal("blocks aren't equal")
	}
	if a.Cid() != b.Cid() {
		t.Fatal("block keys aren't equal")
	}
}
