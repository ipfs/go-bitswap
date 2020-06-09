package wantrequestmanager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"golang.org/x/sync/errgroup"
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

	wg, gctx := errgroup.WithContext(ctx)
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
		wg.Go(func() error {
			select {
			case <-time.After(10 * time.Millisecond):
				return fmt.Errorf("timed out")
			case rcvd := <-incoming:
				if len(rcvd.Blks) != len(incomingBlks) {
					return fmt.Errorf("Expected to receive incoming message with blocks")
				}
			case <-gctx.Done():
				return gctx.Err()
			}

			return nil
		})

		// Receive the blocks on the outgoing channel
		wg.Go(func() error {
			for i := 0; i < len(incomingBlks); i++ {
				select {
				case <-time.After(10 * time.Millisecond):
					return fmt.Errorf("timed out")
				case blk := <-wr.Out:
					if !testutil.ContainsBlock(incomingBlks, blk) {
						return fmt.Errorf("Expected to receive %d incoming messages", len(incomingBlks))
					}
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}

	_, err := wrm.PublishToSessions(&IncomingMessage{
		From: p0,
		Blks: incomingBlks,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
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
	_, err = wrm.PublishToSessions(&IncomingMessage{
		From: p0,
		Blks: incomingBlks,
	})
	if err != nil {
		t.Fatal(err)
	}

	rcvd := receiveMessage()
	if len(rcvd.Blks) != 1 || !testutil.ContainsBlock(rcvd.Blks, incomingBlks[0]) {
		t.Fatal("expected to receive block")
	}

	// Publish HAVE - should be received by listening want request
	_, err = wrm.PublishToSessions(&IncomingMessage{
		From:  p0,
		Haves: haves,
	})
	if err != nil {
		t.Fatal(err)
	}

	rcvd = receiveMessage()
	if !testutil.MatchKeysIgnoreOrder(rcvd.Haves, haves) {
		t.Fatal("expected to receive have")
	}

	// Publish DONT_HAVE - should be received by listening want request
	_, err = wrm.PublishToSessions(&IncomingMessage{
		From:      p0,
		DontHaves: dontHaves,
	})
	if err != nil {
		t.Fatal(err)
	}

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
	err := bstore.Put(blk)
	if err != nil {
		t.Fatal(err)
	}

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
	wg, gctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		select {
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timed out")
		case rcvd := <-incoming:
			if len(rcvd.Blks) != 1 || !testutil.ContainsBlock(rcvd.Blks, blk) {
				t.Fatal("expected to receive block")
			}
		case <-gctx.Done():
			return gctx.Err()
		}

		return nil
	})

	// Receive the block on the outgoing channel
	wg.Go(func() error {
		select {
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timed out")
		case b := <-wr.Out:
			if !b.Cid().Equals(blk.Cid()) {
				t.Fatal("expected to receive block")
			}
		case <-gctx.Done():
			return gctx.Err()
		}

		return nil
	})

	err = wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
