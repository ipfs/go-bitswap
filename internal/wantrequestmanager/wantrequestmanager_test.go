package wantrequestmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func createBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
}

func TestWantRequestManager(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore()

	p0 := testutil.GeneratePeers(1)[0]
	blks := testutil.GenerateBlocksOfSize(3, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	incomingBlks := blks[:2]

	wg := sync.WaitGroup{}
	wrm := New(bstore)

	// Create two want requests that want all the blocks
	for i := 0; i < 2; i++ {
		wr, err := wrm.NewWantRequest(cids, func([]cid.Cid) {})
		if err != nil {
			t.Fatal(err)
		}

		// Listen for the incoming message from each want request
		incoming := make(chan *IncomingMessage)
		go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
			incoming <- msg
		})

		// Receive the incoming message
		go func() {
			wg.Add(1)
			defer wg.Done()

			select {
			case <-time.After(10 * time.Millisecond):
				t.Fatal("timed out")
			case rcvd := <-incoming:
				if len(rcvd.Blks) != len(incomingBlks) {
					t.Fatal("Expected to receive incoming message with blocks")
				}
			}
		}()

		// Receive the blocks on the outgoing channel
		go func() {
			wg.Add(1)
			defer wg.Done()

			for i := 0; i < len(incomingBlks); i++ {
				select {
				case <-time.After(10 * time.Millisecond):
					t.Fatal("timed out")
				case blk := <-wr.Out:
					if !testutil.ContainsBlock(incomingBlks, blk) {
						t.Fatalf("Expected to receive %d incoming messages", len(incomingBlks))
					}
				}
			}
		}()
	}

	wrm.PublishToSessions(&IncomingMessage{
		From: p0,
		Blks: incomingBlks,
	})

	wg.Wait()
}

func TestSubscribeForBlockHaveDontHave(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore()

	p0 := testutil.GeneratePeers(1)[0]
	blks := testutil.GenerateBlocksOfSize(3, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	incomingBlks := blks[:1]
	haves := cids[1:2]
	dontHaves := cids[2:3]

	wrm := New(bstore)

	wr, err := wrm.NewWantRequest(cids, func([]cid.Cid) {})
	if err != nil {
		t.Fatal(err)
	}

	// Listen for the incoming message from each want request
	incoming := make(chan *IncomingMessage)
	go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
		incoming <- msg
	})

	// Read blocks from the Out channel so the run loop doesn't get stuck
	go func() {
		for range wr.Out {
		}
	}()

	// Receive the incoming message
	receiveMessage := func() *IncomingMessage {
		select {
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timed out")
		case rcvd := <-incoming:
			return rcvd
		}
		return nil
	}

	// Publish block - should be received by listening want request
	wrm.PublishToSessions(&IncomingMessage{
		From: p0,
		Blks: incomingBlks,
	})

	rcvd := receiveMessage()
	if len(rcvd.Blks) != 1 || !testutil.ContainsBlock(rcvd.Blks, incomingBlks[0]) {
		t.Fatal("expected to receive block")
	}

	// Publish HAVE - should be received by listening want request
	wrm.PublishToSessions(&IncomingMessage{
		From:  p0,
		Haves: haves,
	})

	rcvd = receiveMessage()
	if !testutil.MatchKeysIgnoreOrder(rcvd.Haves, haves) {
		t.Fatal("expected to receive have")
	}

	// Publish DONT_HAVE - should be received by listening want request
	wrm.PublishToSessions(&IncomingMessage{
		From:      p0,
		DontHaves: dontHaves,
	})

	rcvd = receiveMessage()
	if !testutil.MatchKeysIgnoreOrder(rcvd.DontHaves, dontHaves) {
		t.Fatal("expected to receive dontHave")
	}
}

func TestWantForBlockAlreadyInBlockstore(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore()

	blk := testutil.GenerateBlocksOfSize(1, 8*1024)[0]
	cids := []cid.Cid{blk.Cid()}

	// Put a block into the blockstore
	bstore.Put(blk)

	wrm := New(bstore)

	// Create a want request for the block that is already in the blockstore
	wr, err := wrm.NewWantRequest(cids, func([]cid.Cid) {})
	if err != nil {
		t.Fatal(err)
	}

	// Listen for the incoming message
	incoming := make(chan *IncomingMessage)
	go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
		incoming <- msg
	})

	// WantRequestManager should find the block in the blockstore and
	// immmediately publish a message
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()

		select {
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timed out")
		case rcvd := <-incoming:
			if len(rcvd.Blks) != 1 || !testutil.ContainsBlock(rcvd.Blks, blk) {
				t.Fatal("expected to receive block")
			}
		}
	}()

	// Receive the block on the outgoing channel
	go func() {
		wg.Add(1)
		defer wg.Done()

		select {
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timed out")
		case b := <-wr.Out:
			if !b.Cid().Equals(blk.Cid()) {
				t.Fatal("expected to receive block")
			}
		}
	}()

	wg.Wait()
}

// func TestDuplicates(t *testing.T) {
// 	b1 := blocks.NewBlock([]byte("1"))
// 	b2 := blocks.NewBlock([]byte("2"))

// 	n := New()
// 	defer n.Shutdown()
// 	ch := n.Subscribe(context.Background(), b1.Cid(), b2.Cid())

// 	n.Publish(b1)
// 	blockRecvd, ok := <-ch
// 	if !ok {
// 		t.Fail()
// 	}
// 	assertBlocksEqual(t, b1, blockRecvd)

// 	n.Publish(b1) // ignored duplicate

// 	n.Publish(b2)
// 	blockRecvd, ok = <-ch
// 	if !ok {
// 		t.Fail()
// 	}
// 	assertBlocksEqual(t, b2, blockRecvd)
// }

// func TestPublishSubscribe(t *testing.T) {
// 	blockSent := blocks.NewBlock([]byte("Greetings from The Interval"))

// 	n := New()
// 	defer n.Shutdown()
// 	ch := n.Subscribe(context.Background(), blockSent.Cid())

// 	n.Publish(blockSent)
// 	blockRecvd, ok := <-ch
// 	if !ok {
// 		t.Fail()
// 	}

// 	assertBlocksEqual(t, blockRecvd, blockSent)

// }

// func TestSubscribeMany(t *testing.T) {
// 	e1 := blocks.NewBlock([]byte("1"))
// 	e2 := blocks.NewBlock([]byte("2"))

// 	n := New()
// 	defer n.Shutdown()
// 	ch := n.Subscribe(context.Background(), e1.Cid(), e2.Cid())

// 	n.Publish(e1)
// 	r1, ok := <-ch
// 	if !ok {
// 		t.Fatal("didn't receive first expected block")
// 	}
// 	assertBlocksEqual(t, e1, r1)

// 	n.Publish(e2)
// 	r2, ok := <-ch
// 	if !ok {
// 		t.Fatal("didn't receive second expected block")
// 	}
// 	assertBlocksEqual(t, e2, r2)
// }

// // TestDuplicateSubscribe tests a scenario where a given block
// // would be requested twice at the same time.
// func TestDuplicateSubscribe(t *testing.T) {
// 	e1 := blocks.NewBlock([]byte("1"))

// 	n := New()
// 	defer n.Shutdown()
// 	ch1 := n.Subscribe(context.Background(), e1.Cid())
// 	ch2 := n.Subscribe(context.Background(), e1.Cid())

// 	n.Publish(e1)
// 	r1, ok := <-ch1
// 	if !ok {
// 		t.Fatal("didn't receive first expected block")
// 	}
// 	assertBlocksEqual(t, e1, r1)

// 	r2, ok := <-ch2
// 	if !ok {
// 		t.Fatal("didn't receive second expected block")
// 	}
// 	assertBlocksEqual(t, e1, r2)
// }

// func TestShutdownBeforeUnsubscribe(t *testing.T) {
// 	e1 := blocks.NewBlock([]byte("1"))

// 	n := New()
// 	ctx, cancel := context.WithCancel(context.Background())
// 	ch := n.Subscribe(ctx, e1.Cid()) // no keys provided
// 	n.Shutdown()
// 	cancel()

// 	select {
// 	case _, ok := <-ch:
// 		if ok {
// 			t.Fatal("channel should have been closed")
// 		}
// 	case <-time.After(5 * time.Second):
// 		t.Fatal("channel should have been closed")
// 	}
// }

// func TestSubscribeIsANoopWhenCalledWithNoKeys(t *testing.T) {
// 	n := New()
// 	defer n.Shutdown()
// 	ch := n.Subscribe(context.Background()) // no keys provided
// 	if _, ok := <-ch; ok {
// 		t.Fatal("should be closed if no keys provided")
// 	}
// }

// func TestCarryOnWhenDeadlineExpires(t *testing.T) {

// 	impossibleDeadline := time.Nanosecond
// 	fastExpiringCtx, cancel := context.WithTimeout(context.Background(), impossibleDeadline)
// 	defer cancel()

// 	n := New()
// 	defer n.Shutdown()
// 	block := blocks.NewBlock([]byte("A Missed Connection"))
// 	blockChannel := n.Subscribe(fastExpiringCtx, block.Cid())

// 	assertBlockChannelNil(t, blockChannel)
// }

// func TestDoesNotDeadLockIfContextCancelledBeforePublish(t *testing.T) {

// 	g := blocksutil.NewBlockGenerator()
// 	ctx, cancel := context.WithCancel(context.Background())
// 	n := New()
// 	defer n.Shutdown()

// 	t.Log("generate a large number of blocks. exceed default buffer")
// 	bs := g.Blocks(1000)
// 	ks := func() []cid.Cid {
// 		var keys []cid.Cid
// 		for _, b := range bs {
// 			keys = append(keys, b.Cid())
// 		}
// 		return keys
// 	}()

// 	_ = n.Subscribe(ctx, ks...) // ignore received channel

// 	t.Log("cancel context before any blocks published")
// 	cancel()
// 	for _, b := range bs {
// 		n.Publish(b)
// 	}

// 	t.Log("publishing the large number of blocks to the ignored channel must not deadlock")
// }

// func assertBlockChannelNil(t *testing.T, blockChannel <-chan blocks.Block) {
// 	_, ok := <-blockChannel
// 	if ok {
// 		t.Fail()
// 	}
// }

// func assertBlocksEqual(t *testing.T, a, b blocks.Block) {
// 	if !bytes.Equal(a.RawData(), b.RawData()) {
// 		t.Fatal("blocks aren't equal")
// 	}
// 	if a.Cid() != b.Cid() {
// 		t.Fatal("block keys aren't equal")
// 	}
// }
