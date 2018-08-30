package bitswap

import (
	"context"
	"sync"
	"testing"
	"time"

	tn "github.com/ipfs/go-bitswap/testnet"

	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

// in this test, each of the data-having peers has all the data
func TestDuplicateBlocksIssues(t *testing.T) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(3)
	blocks := bg.Blocks(100)

	bill := instances[0]
	jeff := instances[1]
	steve := instances[2]

	if err := bill.Blockstore().PutMany(blocks); err != nil {
		t.Fatal(err)
	}
	if err := jeff.Blockstore().PutMany(blocks); err != nil {
		t.Fatal(err)
	}

	ses := steve.Exchange.NewSession(context.Background())
	for _, blk := range blocks {
		ses.GetBlock(context.Background(), blk.Cid())
	}

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if st.DupBlksReceived != 0 {
		t.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}

// in this test, each of the 'other peers' has 3/4 of the data. and a 1/2
// overlap in blocks with the other data-having peer
// interestingly, because of the way sessions currently work, this results in zero wasted data
func TestDupBlocksOverlap1(t *testing.T) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(3)
	blocks := bg.Blocks(100)

	bill := instances[0]
	jeff := instances[1]
	steve := instances[2]

	if err := bill.Blockstore().PutMany(blocks[:75]); err != nil {
		t.Fatal(err)
	}
	if err := jeff.Blockstore().PutMany(blocks[25:]); err != nil {
		t.Fatal(err)
	}

	ses := steve.Exchange.NewSession(context.Background())
	for _, blk := range blocks {
		ses.GetBlock(context.Background(), blk.Cid())
	}

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if st.DupBlksReceived != 0 {
		t.Fatal("got duplicate blocks!")
	}
}

// in this test, each of the 'other peers' some of the data, with an overlap
// different from the previous test, both peers have the 'first' block, which triggers sessions
// into behaving poorly
func TestDupBlocksOverlap2(t *testing.T) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(3)
	blocks := bg.Blocks(100)

	bill := instances[0]
	jeff := instances[1]
	steve := instances[2]

	bill.Blockstore().Put(blocks[0])
	jeff.Blockstore().Put(blocks[0])
	for i, blk := range blocks {
		if i%3 == 0 {
			bill.Blockstore().Put(blk)
			jeff.Blockstore().Put(blk)
		} else if i%2 == 1 {
			bill.Blockstore().Put(blk)
		} else {
			jeff.Blockstore().Put(blk)
		}
	}

	ses := steve.Exchange.NewSession(context.Background())
	for _, blk := range blocks {
		ses.GetBlock(context.Background(), blk.Cid())
	}

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if st.DupBlksReceived != 0 {
		t.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}

// in this test, each of the 'other peers' some of the data, with an overlap
// The data is fetched in bulk, with a single 'getBlocks' call
func TestDupBlocksOverlapBatch1(t *testing.T) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(3)
	blocks := bg.Blocks(100)

	bill := instances[0]
	jeff := instances[1]
	steve := instances[2]

	bill.Blockstore().Put(blocks[0])
	jeff.Blockstore().Put(blocks[0])
	for i, blk := range blocks {
		if i%3 == 0 {
			bill.Blockstore().Put(blk)
			jeff.Blockstore().Put(blk)
		} else if i%2 == 1 {
			bill.Blockstore().Put(blk)
		} else {
			jeff.Blockstore().Put(blk)
		}
	}

	ses := steve.Exchange.NewSession(context.Background())

	var ks []*cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}
	out, err := ses.GetBlocks(context.Background(), ks)
	if err != nil {
		t.Fatal(err)
	}
	for range out {
	}

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if st.DupBlksReceived != 0 {
		t.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}

// in this test, each of the 'other peers' some of the data, with an overlap
// The data is fetched in bulk, with N concurrent calls to 'getBlock'
func TestDupBlocksOverlapBatch2(t *testing.T) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(3)
	blocks := bg.Blocks(100)

	bill := instances[0]
	jeff := instances[1]
	steve := instances[2]

	bill.Blockstore().Put(blocks[0])
	jeff.Blockstore().Put(blocks[0])
	for i, blk := range blocks {
		if i%3 == 0 {
			bill.Blockstore().Put(blk)
			jeff.Blockstore().Put(blk)
		} else if i%2 == 1 {
			bill.Blockstore().Put(blk)
		} else {
			jeff.Blockstore().Put(blk)
		}
	}

	ses := steve.Exchange.NewSession(context.Background())

	var wg sync.WaitGroup
	for _, blk := range blocks {
		wg.Add(1)
		go func(c *cid.Cid) {
			defer wg.Done()
			_, err := ses.GetBlock(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
		}(blk.Cid())
	}
	wg.Wait()

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if st.DupBlksReceived != 0 {
		t.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}

// in this test, each of the 'other peers' some of the data, with an overlap
// The data is fetched in bulk, fetching ten blocks at a time with 'getBlocks'
func TestDupBlocksOverlapBatch3(t *testing.T) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(3)
	blocks := bg.Blocks(100)

	bill := instances[0]
	jeff := instances[1]
	steve := instances[2]

	bill.Blockstore().Put(blocks[0])
	jeff.Blockstore().Put(blocks[0])
	for i, blk := range blocks {
		if i%3 == 0 {
			bill.Blockstore().Put(blk)
			jeff.Blockstore().Put(blk)
		} else if i%2 == 1 {
			bill.Blockstore().Put(blk)
		} else {
			jeff.Blockstore().Put(blk)
		}
	}

	ses := steve.Exchange.NewSession(context.Background())

	var ks []*cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}

	for i := 0; i < len(ks); i += 10 {
		out, err := ses.GetBlocks(context.Background(), ks[i:i+10])
		if err != nil {
			t.Fatal(err)
		}
		for range out {
		}
	}

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if st.DupBlksReceived != 0 {
		t.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}
