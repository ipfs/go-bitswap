package bitswap

import (
	"context"
	"testing"
	"time"

	tn "github.com/ipfs/go-bitswap/testnet"

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
