package wantrequestmanager

import (
	"context"
	"fmt"
	"testing"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
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
	wrm := New(bstore, bsbpm.New())

	// Create two want requests that want all the blocks
	for i := 0; i < 2; i++ {
		wr, err := wrm.NewWantRequest(cids)
		if err != nil {
			t.Fatal(err)
		}

		// Listen for the incoming message from each want request
		incoming := make(chan *IncomingMessage)
		go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
			incoming <- msg
		}, func([]cid.Cid) {})

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

	_, err := wrm.ReceiveMessage(&IncomingMessage{
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
	blks := testutil.GenerateBlocksOfSize(5, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	// Incoming blocks, HAVEs and DONT_HAVEs
	incomingBlks := blks[:2]
	haves := cids[2:4]
	dontHaves := cids[4:6]

	// Only interested in one of each
	wants := []cid.Cid{cids[0], cids[2], cids[4]}

	wrm := New(bstore, bsbpm.New())

	wr, err := wrm.NewWantRequest(wants)
	if err != nil {
		t.Fatal(err)
	}

	// Listen for the incoming message from each want request
	incoming := make(chan *IncomingMessage)
	go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
		incoming <- msg
	}, func([]cid.Cid) {})

	// Add another WantRequest that is not interested in the CIDs
	// (shouldn't receive anything)
	wrOther, err := wrm.NewWantRequest(testutil.GenerateCids(2))
	if err != nil {
		t.Fatal(err)
	}
	otherIncoming := make(chan *IncomingMessage)
	go wrOther.Run(ctx, ctx, func(msg *IncomingMessage) {
		otherIncoming <- msg
	}, func([]cid.Cid) {})

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
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From: p0,
		Blks: incomingBlks,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Only interested in first block
	rcvd := receiveMessage()
	if len(rcvd.Blks) != 1 || !testutil.ContainsBlock(rcvd.Blks, incomingBlks[0]) {
		t.Fatal("expected to receive block")
	}

	// Publish HAVE - should be received by listening want request
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From:  p0,
		Haves: haves,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Only interested in first HAVE
	rcvd = receiveMessage()
	if !testutil.MatchKeysIgnoreOrder(rcvd.Haves, haves[:1]) {
		t.Fatal("expected to receive have")
	}

	// Publish DONT_HAVE - should be received by listening want request
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From:      p0,
		DontHaves: dontHaves,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Only interested in first DONT_HAVE
	rcvd = receiveMessage()
	if !testutil.MatchKeysIgnoreOrder(rcvd.DontHaves, dontHaves[:1]) {
		t.Fatal("expected to receive dontHave")
	}

	// Other WantRequest is not interested in any of the keys so should not
	// receive anything
	if len(otherIncoming) > 0 {
		t.Fatal("expected other WantRequest not to receive anything")
	}
}

func TestUpdatesBlockPresenceManager(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore()
	bpm := bsbpm.New()
	wrm := New(bstore, bpm)

	p0 := testutil.GeneratePeers(1)[0]
	blks := testutil.GenerateBlocksOfSize(2, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	haves := cids[:1]
	dontHaves := cids[1:]

	wr, err := wrm.NewWantRequest(cids)
	if err != nil {
		t.Fatal(err)
	}

	// Listen for the incoming message from each want request
	incoming := make(chan *IncomingMessage)
	go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
		incoming <- msg
	}, func([]cid.Cid) {})

	// Publish message with 1 HAVE and 1 DONT_HAVE
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From:      p0,
		Haves:     haves,
		DontHaves: dontHaves,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Receive the incoming message
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("timed out")
	case <-incoming:
		if !bpm.PeerHasBlock(p0, haves[0]) || !bpm.PeerDoesNotHaveBlock(p0, dontHaves[0]) {
			t.Fatal("did not register HAVEs / DONT_HAVEs with BlockPresenceManager")
		}
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

	wrm := New(bstore, bsbpm.New())

	// Create a want request for the block that is already in the blockstore
	wr, err := wrm.NewWantRequest(cids)
	if err != nil {
		t.Fatal(err)
	}

	// Listen for the incoming message
	incoming := make(chan *IncomingMessage)
	go wr.Run(ctx, ctx, func(msg *IncomingMessage) {
		incoming <- msg
	}, func([]cid.Cid) {})

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

func TestPublishWanted(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore()

	p0 := testutil.GeneratePeers(1)[0]
	blks := testutil.GenerateBlocksOfSize(3, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	wrm := New(bstore, bsbpm.New())

	wr, err := wrm.NewWantRequest(cids)
	if err != nil {
		t.Fatal(err)
	}
	go wr.Run(ctx, ctx, func(msg *IncomingMessage) {}, func([]cid.Cid) {})

	// Publish first block
	wanted, err := wrm.ReceiveMessage(&IncomingMessage{
		From: p0,
		Blks: blks[:1],
	})
	if err != nil {
		t.Fatal(err)
	}
	// Expect first block to be wanted
	if wanted.Len() != 1 {
		t.Fatal("expected block to be wanted")
	}

	// Publish first two blocks
	wanted, err = wrm.ReceiveMessage(&IncomingMessage{
		From: p0,
		Blks: blks[:2],
	})
	if err != nil {
		t.Fatal(err)
	}
	// Expect only second block to be wanted
	if wanted.Len() != 1 {
		t.Fatal("expected one of two blocks to be wanted")
	}
}

func TestWantRequestCancelled(t *testing.T) {
	ctx := context.Background()
	reqctx, cancel := context.WithCancel(ctx)
	testCancel(t, ctx, reqctx, cancel)
}

func TestSessionCancelled(t *testing.T) {
	ctx := context.Background()
	sessctx, cancel := context.WithCancel(ctx)
	testCancel(t, sessctx, ctx, cancel)
}

func testCancel(t *testing.T, sessctx context.Context, reqctx context.Context, cancelfn func()) {
	bstore := createBlockstore()

	p0 := testutil.GeneratePeers(1)[0]
	blks := testutil.GenerateBlocksOfSize(3, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	wrm := New(bstore, bsbpm.New())

	wr, err := wrm.NewWantRequest(cids)
	if err != nil {
		t.Fatal(err)
	}

	// Pop incoming messages
	cancelled := make(chan []cid.Cid)
	go wr.Run(sessctx, reqctx, func(msg *IncomingMessage) {}, func(cancels []cid.Cid) {
		cancelled <- cancels
	})
	// Pop outgoing blocks
	go func() {
		for range wr.Out {
		}
	}()

	// Publish first block
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From: p0,
		Blks: blks[:1],
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)

	// Cancel context
	cancelfn()

	time.Sleep(10 * time.Millisecond)

	// Should call cancel function with remaining blocks
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("timed out")
	case cancels := <-cancelled:
		if len(cancels) != len(blks)-1 {
			t.Fatal("received wrong number of cancels", len(cancels))
		}
	}

	// Should close outgoing channel
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("timed out")
	case _, ok := <-wr.Out:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	}
}

func TestAllBlocksReceived(t *testing.T) {
	ctx := context.Background()
	bstore := createBlockstore()

	p0 := testutil.GeneratePeers(1)[0]
	blks := testutil.GenerateBlocksOfSize(3, 8*1024)
	cids := []cid.Cid{}
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	wrm := New(bstore, bsbpm.New())

	wr, err := wrm.NewWantRequest(cids)
	if err != nil {
		t.Fatal(err)
	}

	// Listen for incoming messages
	incoming := make(chan *IncomingMessage)
	cancelled := make(chan []cid.Cid)
	reqctx, cancel := context.WithCancel(ctx)
	go wr.Run(ctx, reqctx, func(msg *IncomingMessage) {
		incoming <- msg
	}, func(cancels []cid.Cid) {
		cancelled <- cancels
	})

	// Publish all blocks
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From: p0,
		Blks: blks,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should receiving incoming message
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("timed out")
	case <-incoming:
	}

	// Should receive blocks on outgoing channel then it should be closed
	rcvd := 0
	for range wr.Out {
		rcvd++
	}
	if rcvd != len(blks) {
		t.Fatal("expected to send all blocks on outgoing channel")
	}

	// Publish another message
	_, err = wrm.ReceiveMessage(&IncomingMessage{
		From: p0,
		Blks: blks,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should receive message even though all blocks have already been received
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("timed out")
	case <-incoming:
	}

	// Cancel request
	cancel()

	time.Sleep(10 * time.Millisecond)

	// Should call cancel function with no keys
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("timed out")
	case ks := <-cancelled:
		if len(ks) != 0 {
			t.Fatal("expected no pending wants")
		}
	}
}
