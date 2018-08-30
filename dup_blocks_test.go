package bitswap

import (
	"context"
	"fmt"
	"testing"
	"time"

	tn "github.com/ipfs/go-bitswap/testnet"

	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

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
	for i, blk := range blocks {
		fmt.Println("fetch block: ", i)
		ses.GetBlock(context.Background(), blk.Cid())
	}

	st, err := steve.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("duplicate blocks: ", st.DupBlksReceived)
}
