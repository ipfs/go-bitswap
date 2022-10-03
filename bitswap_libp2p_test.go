package bitswap_test

import (
	"context"
	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestTwoMocknetPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	logging.SetLogLevel("bitswap-server", "debug")

	opts := []bitswap.Option{
		bitswap.TaskWorkerCount(1),
		bitswap.EngineTaskWorkerCount(1),
		bitswap.WithTargetMessageSize(32 << 20),
		bitswap.MaxOutstandingBytesPerPeer(32 << 20),
	}
	t.Run("2 blocks", func(t *testing.T) {
		testTwoMocknetPeers(ctx, t, 2, opts...)
	})
	t.Run("4 blocks", func(t *testing.T) {
		testTwoMocknetPeers(ctx, t, 4, opts...)
	})
}

func TestTwoLibp2pPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logging.SetLogLevel("bitswap-server", "debug")

	opts := []bitswap.Option{
		bitswap.TaskWorkerCount(1),
		bitswap.EngineTaskWorkerCount(1),
		bitswap.WithTargetMessageSize(32 << 20),
		bitswap.MaxOutstandingBytesPerPeer(32 << 20),
	}
	t.Run("2 blocks", func(t *testing.T) {
		testTwoLibp2pPeers(ctx, t, 2, opts...)
	})
	t.Run("4 blocks", func(t *testing.T) {
		testTwoLibp2pPeers(ctx, t, 4, opts...)
	})
}

func TestTwoLibp2pPeersConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logging.SetLogLevel("engine", "debug")
	logging.SetLogLevel("bitswap-server", "debug")
	opts := []bitswap.Option{
		bitswap.TaskWorkerCount(2),
		bitswap.EngineTaskWorkerCount(2),
		bitswap.MaxOutstandingBytesPerPeer(32 << 20),
	}
	testTwoLibp2pPeers(ctx, t, 4, opts...)
}

func testTwoMocknetPeers(ctx context.Context, t *testing.T, numBlocks int, opts ...bitswap.Option) {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0))
	ig := testinstance.NewTestInstanceGenerator(net, nil, opts)
	defer ig.Close()

	instances := ig.Instances(2)
	bsClient := &bsHostInfo{
		bstore: instances[0].Blockstore(),
		bs:     instances[0].Exchange,
	}
	bsServer := &bsHostInfo{
		bstore: instances[1].Blockstore(),
		bs:     instances[1].Exchange,
	}
	testTwoPeers(ctx, t, bsClient, bsServer, numBlocks)
}

func testTwoLibp2pPeers(ctx context.Context, t *testing.T, numBlocks int, opts ...bitswap.Option) {
	clientHost, srvHost := setupLibp2pHosts(t)

	bsClient, err := setupBitswapHost(ctx, clientHost, opts, delay.Fixed(0))
	require.NoError(t, err)
	bsServer, err := setupBitswapHost(ctx, srvHost, opts, delay.Fixed(100*time.Millisecond))
	require.NoError(t, err)

	err = clientHost.Connect(ctx, peer.AddrInfo{
		ID:    srvHost.ID(),
		Addrs: srvHost.Addrs(),
	})
	require.NoError(t, err)

	testTwoPeers(ctx, t, bsClient, bsServer, numBlocks)
}

func testTwoPeers(ctx context.Context, t *testing.T, bsClient, bsServer *bsHostInfo, numBlocks int) {
	blks := genBlocks(numBlocks, 1<<20)

	t.Log("Put the blocks to the server")

	var blkeys []cid.Cid
	for _, b := range blks {
		blkeys = append(blkeys, b.Cid())
		err := bsServer.bstore.Put(ctx, b)
		require.NoError(t, err)
		err = bsServer.bs.NotifyNewBlocks(ctx, b)
		require.NoError(t, err)
	}

	t.Log("Get the blocks from the client")

	outch, err := bsClient.bs.GetBlocks(ctx, blkeys)
	require.NoError(t, err)

	var receivedCount int
	for range outch {
		receivedCount++
	}
	require.Equal(t, numBlocks, receivedCount)
}

type bsHostInfo struct {
	bstore blockstore.Blockstore
	bs     *bitswap.Bitswap
}

func setupBitswapHost(ctx context.Context, clientHost host.Host, opts []bitswap.Option, bstoreDelay delay.D) (*bsHostInfo, error) {
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bstoreDelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))
	routing, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	bsn := network.NewFromIpfsHost(clientHost, routing)
	bs := bitswap.New(ctx, bsn, bstore, opts...)
	return &bsHostInfo{
		bstore: bstore,
		bs:     bs,
	}, nil
}

func setupLibp2pHosts(t *testing.T) (host.Host, host.Host) {
	m1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	m2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	srvHost := newHost(t, m1)
	clientHost := newHost(t, m2)

	srvHost.Peerstore().AddAddrs(clientHost.ID(), clientHost.Addrs(), peerstore.PermanentAddrTTL)
	clientHost.Peerstore().AddAddrs(srvHost.ID(), srvHost.Addrs(), peerstore.PermanentAddrTTL)

	return clientHost, srvHost
}

func newHost(t *testing.T, listen multiaddr.Multiaddr) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrs(listen))
	require.NoError(t, err)
	return h
}

func genBlocks(numBlocks int, sz int) []*blocks.BasicBlock {
	blks := make([]*blocks.BasicBlock, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		bz := make([]byte, sz)
		rand.Read(bz)
		blks = append(blks, blocks.NewBlock(bz))
	}
	return blks
}
