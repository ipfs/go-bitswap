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

func createBlockstore(t *testing.T, ctx context.Context) blockstore.Blockstore {
	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore())),
		blockstore.DefaultCacheOpts())
	if err != nil {
		t.Fatal(err)
	}

	return bstore
}

func TestWantRequestManager(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore(t, ctx)

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
	bstore := createBlockstore(t, ctx)

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
	bstore := createBlockstore(t, ctx)

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
