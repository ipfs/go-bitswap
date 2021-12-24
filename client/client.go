package client

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
)

var (
	ProtocolBitswapNoVers  protocol.ID = "/ipfs/bitswap"
	ProtocolBitswapOneZero protocol.ID = "/ipfs/bitswap/1.0.0"
	ProtocolBitswapOneOne  protocol.ID = "/ipfs/bitswap/1.1.0"
	ProtocolBitswap        protocol.ID = "/ipfs/bitswap/1.2.0"
)

var _ exchange.Fetcher = (*Client)(nil)

type response struct {
	blk blocks.Block
}

type Client struct {
	ProviderStore providers.ProviderStore
	Host          host.Host
	Blockstore    blockstore.Blockstore

	wantsMut sync.RWMutex
	// map from (cid,peer) -> requestid -> response chan
	wants map[string]map[string]chan response
}

func (c *Client) getBlockFromPeer(ctx context.Context, cid cid.Cid, peer peer.ID) (blocks.Block, error) {
	stream, err := c.Host.NewStream(ctx, peer, ProtocolBitswap)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	// send request
	msg := message.New(true)
	// ask for a dont-have, and assume the server returns with it
	// this may not be true in the large, but for now this is a super naive impl
	msg.AddEntry(cid, 10, pb.Message_Wantlist_Block, true)

	// make the response channel and add it to the response map
	// the resp handler will remove the channel after sending a value on it
	ch := make(chan response, 1)
	c.wantsMut.Lock()
	reqID := uuid.New().String()
	key := wantsKey(cid, peer)
	if _, ok := c.wants[key]; !ok {
		c.wants[key] = map[string]chan response{}
	}
	c.wants[key][reqID] = ch
	c.wantsMut.Unlock()

	// write the request
	err = msg.ToNetV1(stream)
	if err != nil {
		// TODO: this is a problematic case currently, if there's an error we might be leaking channels into the map
		return nil, err
	}
	if err := stream.Close(); err != nil {
		log.Printf("error closing stream: %s", err.Error())
	}

	// wait on the response
	resp := <-ch
	return resp.blk, nil
}

func wantsKey(cid cid.Cid, peerID peer.ID) string {
	return fmt.Sprintf("%s,%s", cid.String(), peerID.String())
}

func (c *Client) handleStream(stream network.Stream) {
	defer func() {
		if err := stream.Close(); err != nil {
			log.Print("error closing stream")
		}
	}()
	respMsg, err := message.FromNet(stream)
	if err != nil {
		log.Printf("error reading message from stream, discarding: %s", err.Error())
	}
	c.wantsMut.Lock()
	defer c.wantsMut.Unlock()
	toDelete := map[string]string{}
	for _, dontHave := range respMsg.DontHaves() {
		k := wantsKey(dontHave, stream.Conn().RemotePeer())
		if chanMap, ok := c.wants[k]; ok {
			for reqID, ch := range chanMap {
				ch <- response{}
				toDelete[k] = reqID
			}
		}
	}
	for _, blk := range respMsg.Blocks() {
		k := wantsKey(blk.Cid(), stream.Conn().RemotePeer())
		if chanMap, ok := c.wants[k]; ok {
			for reqID, ch := range chanMap {
				ch <- response{blk: blk}
				toDelete[k] = reqID
			}
		}
	}
	// remove channels we've sent values on
	for k, reqID := range toDelete {
		delete(c.wants[k], reqID)
	}
}

func (c *Client) GetBlock(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	// naive impl: send want-block to each peer sequentially
	peers := c.Host.Peerstore().Peers()
	for _, peer := range peers {
		if peer == c.Host.ID() {
			continue
		}
		blk, err := c.getBlockFromPeer(ctx, cid, peer)
		if err != nil {
			return nil, err
		}
		if blk != nil {
			return blk, nil
		}
	}
	return nil, nil
}

func (c *Client) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	panic("not implemented") // TODO: Implement
}

func New(providerStore providers.ProviderStore, host host.Host, blockstore blockstore.Blockstore) *Client {
	c := &Client{
		ProviderStore: providerStore,
		Host:          host,
		Blockstore:    blockstore,
		wants:         map[string]map[string]chan response{},
	}
	c.Host.SetStreamHandler(ProtocolBitswap, c.handleStream)
	c.Host.SetStreamHandler(ProtocolBitswapNoVers, c.handleStream)
	c.Host.SetStreamHandler(ProtocolBitswapOneOne, c.handleStream)
	c.Host.SetStreamHandler(ProtocolBitswapOneZero, c.handleStream)
	return c
}
