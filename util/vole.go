package util

// Fixed peer ID from where we make the requests to make it easier to track:
//   `12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN`
// (Libp2p doesn't do self connections so we create a different peer ID instead
// of using the same target peer, localhost, on where we request the CIDs.)

import (
	"math/rand"
	"math"
	"strconv"

	"context"
	"fmt"
	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	quic "github.com/libp2p/go-libp2p-quic-transport"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsmsgpb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	nrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-tcp-transport"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

var bsLog = logging.Logger("bitswap")

// Map of CIDs to be requested and whether they
// had been answered or not (in each test round).
var testCIDs map[cid.Cid]bool

func init() {
	// Generate fixed public peer ID.
	var err error
	privKey, _, err = crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, new(zeroReader))
	if err != nil {
		panic(err)
	}
}

// FIXME: There is probably a much easier way to get unlimited zero bytes.
var privKey crypto.PrivKey
type zeroReader struct {
}
func (*zeroReader) Read(p []byte) (n int, err error) {
	p[0] = 0
	return 1, nil
}
// FIXME: Rename.
func CheckBitswapCID(ctx context.Context,
	targetPeer peer.ID, cidNum int) (map[cid.Cid]bool, error) {
	if cidNum == 0 {
		panic("cidNum set to zero")
	}
	if len(testCIDs) != cidNum { // we create twice cidNum, existent and nonexistent ones
		// The number of CIDs to request has been updated.
		testCIDs = createCids(cidNum)
		// FIXME: If `cidNum` is odd it will never match the size of `testCIDs`
		//  which is constructed as an even division.
	}

	rcv := &bsReceiver{
		target: targetPeer,
		result: make(chan msgOrErr),
	}
	bs, err := connectToBitSwap(ctx, targetPeer, rcv)
	if err != nil {
		return nil, err
	}

	msg := createMsg(testCIDs)

	if err := bs.SendMessage(ctx, targetPeer, msg); err != nil {
		return nil, err
	}

	return trackResponses(ctx, rcv, testCIDs)
}

// We will wait for the responses as long as the passed context dictates.
// FIXME: We could also set another timeout here but centralize in caller
//  for now for simplicity.
// FIXME: We should definitely track discrepancies (cases where nodes report having but don't actually send back data).
func trackResponses(ctx context.Context, rcv *bsReceiver, requestedCids map[cid.Cid]bool) (map[cid.Cid]bool, error) {
	for c, _ := range requestedCids {
		requestedCids[c] = false
	}

	// Keep track of responded CIDs to exit early.
	responded := 0
	markResponse := func(c cid.Cid) {
		if requestedCids[c] == false {
			requestedCids[c] = true
			responded += 1
		}
	}

loop:
	for {
		var res msgOrErr
		select {
		case res = <-rcv.result:
		case <-ctx.Done():
			break loop
		}

		if res.err != nil {
			return nil, res.err
		}

		if res.msg == nil {
			panic("should not be reachable")
		}

		for _, block := range res.msg.Blocks() {
			markResponse(block.Cid())
		}

		for _, c := range res.msg.Haves() {
			markResponse(c)
		}

		for _, c := range res.msg.DontHaves() {
			markResponse(c)
		}
		if responded == len(requestedCids) {
			return nil, nil
		}
	}

	missingCids := make(map[cid.Cid]bool)
	for c, responded := range requestedCids {
		if !responded {
			missingCids[c] = true
		}
	}
	return missingCids, nil
}

func connectToBitSwap(ctx context.Context, targetPeer peer.ID, rcv *bsReceiver) (bsnet.BitSwapNetwork, error) {
	// Connect randomly on either TCP or QUIC. QUIC has been the known source
	// of error so far but we still try both just in case.
	// See https://github.com/ipfs/go-bitswap/pull/477 for QUIC stall fix.
	localhost := ""
	if rand.Intn(2) == 1 {
		localhost = "/ip4/127.0.0.1/udp/4001/quic/p2p/" + targetPeer.String()
		bsLog.Infof("connecting through QUIC")
	} else {
		localhost = "/ip4/127.0.0.1/tcp/4001/p2p/" + targetPeer.String()
		bsLog.Infof("connecting through TCP")
	}

	ma, err := multiaddr.NewMultiaddr(localhost)
	if err != nil {
		panic("failed to decode multi-address")
	}

	// FIXME: Can we reuse the `bitswapNetwork` in the IPFS core node
	//  constructor? We would need to overwrite the receive delegate
	//  (which is controlled by the BitSwap struct) or chain them
	//  somehow. Right now we are re-connecting to ourselves at the tcp/libp2p
	//  level creating a new host. (Maybe in terms of testing including this
	//  layer, even at the bypassed localhost level, is useful.)
	h, err := libp2p.New(ctx,
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),

		libp2p.Identity(privKey), // ID: 12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN
	)
	if err != nil {
		return nil, err
	}

	ai, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		return nil, err
	}

	hctx, _ := context.WithTimeout(ctx, time.Second*1) // Basic connection timeout for localhost.
	if err := h.Connect(hctx, *ai); err != nil {
		return nil, err
	}

	nilRouter, err := nrouting.ConstructNilRouting(nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	bs := bsnet.NewFromIpfsHost(h, nilRouter)

	bs.SetDelegate(rcv)

	return bs, nil
}

// We create two types of CIDs.
// * ID hashes present by default. (Also they will count as `Work` size in the
//   task entry for the priority as the size of the CID/block.)
// * Raw invalid (shortened) SHA-256 CIDs that guarantee nonexistence and helps
//  to test the counterpart of the ID CIDs which will always be found.
// FIXME: We should have different sizes of ID hashes.
// FIXME: Potential issues:
//  * Won't rule out datastore/blockstore issues.
//  * Won't rule out issues where a node doesn't have a block then later gets a copy.
func createCids(cidNum int) map[cid.Cid]bool {
	cids := make(map[cid.Cid]bool, cidNum)

	v1RawIDPrefix := cid.Prefix{
		Version: 1,
		Codec:   cid.Raw,
		MhType:  mh.IDENTITY,
	}

	v1RawSha256Prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: 20, // To guarantee nonexistence shorten the 32-byte SHA length.
	}

	cidsPerGroup := int(math.Ceil(float64(cidNum) / 2))
	for i := 0; i < cidsPerGroup; i++ {
		c, err := v1RawIDPrefix.Sum([]byte(strconv.FormatUint(uint64(i), 10)))
		if err != nil {
			panic("error creating raw ID CID")
		}
		cids[c] = false
		c, err = v1RawSha256Prefix.Sum([]byte(strconv.FormatUint(uint64(i), 10)))
		if err != nil {
			panic("error creating raw SHA-256 CID")
		}
		cids[c] = false
	}
	return cids
}

func createMsg(requestedCids map[cid.Cid]bool) bsmsg.BitSwapMessage {
	msg := bsmsg.New(true)
	// Creating a new full list to replace the previous one since each test
	// is independent.
	for c, _ := range requestedCids {
		wantType := bsmsgpb.Message_Wantlist_Have
		if rand.Intn(2) == 1 {
			wantType = bsmsgpb.Message_Wantlist_Block
		}
		msg.AddEntry(c, rand.Int31(), wantType, true)
		// Priority is only local to peer so not relevant for this test, still
		// keep it random just in case to avoid bias.
	}
	return msg
}

type bsReceiver struct {
	target peer.ID
	result chan msgOrErr
}

type msgOrErr struct {
	msg bsmsg.BitSwapMessage
	err error
}

func (r *bsReceiver) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	if r.target != sender {
		select {
		case <-ctx.Done():
		case r.result <- msgOrErr{err: fmt.Errorf("expected peerID %v, got %v", r.target, sender)}:
		}
		return
	}

	select {
	case <-ctx.Done():
	case r.result <- msgOrErr{msg: incoming}:
	}
}

func (r *bsReceiver) ReceiveError(err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
	case r.result <- msgOrErr{err: err}:
	}
}

func (r *bsReceiver) PeerConnected(id peer.ID) {
	return
}

func (r *bsReceiver) PeerDisconnected(id peer.ID) {
	return
}

var _ bsnet.Receiver = (*bsReceiver)(nil)
