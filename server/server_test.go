package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	logging "github.com/ipfs/go-log/v2"
	libp2p "github.com/libp2p/go-libp2p"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/stretchr/testify/require"
)

func init() {
	logging.SetLogLevel("bitswap-server", "debug")
}

func TestServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bitswapID := tnet.RandIdentityOrFatal(t)
	bitswapHost, err := libp2p.New(libp2p.Identity(bitswapID.PrivateKey()))
	require.NoError(t, err)
	defer bitswapHost.Close()

	// Not using mocknet because its streams don't support deadlines
	// TODO: add deadlines to mocknet
	//
	//	mockNet := mocknet.New(ctx)
	// bitswapHost, err := mockNet.AddPeer(bitswapID.PrivateKey(), bitswapID.Address())
	// serverID := tnet.RandIdentityOrFatal(t)
	//	serverHost, err := mockNet.AddPeer(serverID.PrivateKey(), serverID.Address())
	// _, err = mockNet.LinkPeers(bitswapHost.ID(), serverHost.ID())
	// require.NoError(t, err)
	// _, err = mockNet.ConnectPeers(bitswapHost.ID(), serverHost.ID())

	serverID := tnet.RandIdentityOrFatal(t)
	serverHost, err := libp2p.New(libp2p.Identity(serverID.PrivateKey()))
	require.NoError(t, err)
	defer serverHost.Close()

	rs := mockrouting.NewServer()

	routingClientBitswap := rs.Client(bitswapID)

	bitswapBlockstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))

	bsNetwork := bsnet.NewFromIpfsHost(bitswapHost, routingClientBitswap)
	defer bsNetwork.Stop()

	bs := bitswap.New(ctx, bsNetwork, bitswapBlockstore)
	defer bs.Close()

	serverBlockstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	server, err := New(serverHost, serverBlockstore)
	require.NoError(t, err)
	defer server.Close()

	numBlocks := 1000
	var blks []blocks.Block
	for i := 0; i < numBlocks; i++ {
		blks = append(blks, blocks.NewBlock([]byte(fmt.Sprintf("block-%d", i))))
		err := serverBlockstore.Put(ctx, blks[i])
		require.NoError(t, err)
	}

	err = bitswapHost.Connect(ctx, serverHost.Peerstore().PeerInfo(serverHost.ID()))

	require.NoError(t, err)

	wg := sync.WaitGroup{}
	for i := 0; i < numBlocks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			received, err := bs.GetBlock(ctx, blks[i].Cid())
			require.NoError(t, err)
			require.Equal(t, blks[i].RawData(), received.RawData())

		}(i)
	}
	wg.Wait()
}
