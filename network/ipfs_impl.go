package network

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"

	ggio "github.com/gogo/protobuf/io"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	routing "github.com/libp2p/go-libp2p-routing"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("bitswap_network")

var sendMessageTimeout = time.Minute * 10

// NewFromIpfsHost returns a BitSwapNetwork supported by underlying IPFS host.
func NewFromIpfsHost(host host.Host, r routing.ContentRouting) BitSwapNetwork {
	bitswapNetwork := impl{
		host:    host,
		routing: r,
	}
	host.SetStreamHandler(ProtocolBitswap, bitswapNetwork.handleNewStream)
	host.SetStreamHandler(ProtocolBitswapOne, bitswapNetwork.handleNewStream)
	host.SetStreamHandler(ProtocolBitswapNoVers, bitswapNetwork.handleNewStream)
	host.Network().Notify((*netNotifiee)(&bitswapNetwork))
	// TODO: StopNotify.

	return &bitswapNetwork
}

// impl transforms the ipfs network interface, which sends and receives
// NetMessage objects, into the bitswap network interface.
type impl struct {
	host    host.Host
	routing routing.ContentRouting

	// inbound messages from the network are forwarded to the receiver
	receiver Receiver

	stats NetworkStats
}

type streamMessageSender struct {
	s inet.Stream
}

func (s *streamMessageSender) Close() error {
	return inet.FullClose(s.s)
}

func (s *streamMessageSender) Reset() error {
	return s.s.Reset()
}

func (s *streamMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	return msgToStream(ctx, s.s, msg)
}

func msgToStream(ctx context.Context, s inet.Stream, msg bsmsg.BitSwapMessage) error {
	deadline := time.Now().Add(sendMessageTimeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}
	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warningf("error setting deadline: %s", err)
	}

	w := bufio.NewWriter(s)

	switch s.Protocol() {
	case ProtocolBitswap:
		if err := msg.ToNetV1(w); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	case ProtocolBitswapOne, ProtocolBitswapNoVers:
		if err := msg.ToNetV0(w); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	if err := w.Flush(); err != nil {
		log.Debugf("error: %s", err)
		return err
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warningf("error resetting deadline: %s", err)
	}
	return nil
}

func (bsnet *impl) NewMessageSender(ctx context.Context, p peer.ID) (MessageSender, error) {
	s, err := bsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return nil, err
	}

	return &streamMessageSender{s: s}, nil
}

func (bsnet *impl) newStreamToPeer(ctx context.Context, p peer.ID) (inet.Stream, error) {
	return bsnet.host.NewStream(ctx, p, ProtocolBitswap, ProtocolBitswapOne, ProtocolBitswapNoVers)
}

func (bsnet *impl) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing bsmsg.BitSwapMessage) error {

	s, err := bsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return err
	}

	if err = msgToStream(ctx, s, outgoing); err != nil {
		s.Reset()
		return err
	}
	atomic.AddUint64(&bsnet.stats.MessagesSent, 1)

	// TODO(https://github.com/libp2p/go-libp2p-net/issues/28): Avoid this goroutine.
	go inet.AwaitEOF(s)
	return s.Close()

}

func (bsnet *impl) SetDelegate(r Receiver) {
	bsnet.receiver = r
}

func (bsnet *impl) ConnectTo(ctx context.Context, p peer.ID) error {
	return bsnet.host.Connect(ctx, pstore.PeerInfo{ID: p})
}

// FindProvidersAsync returns a channel of providers for the given key.
func (bsnet *impl) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.ID {

	// Since routing queries are expensive, give bitswap the peers to which we
	// have open connections. Note that this may cause issues if bitswap starts
	// precisely tracking which peers provide certain keys. This optimization
	// would be misleading. In the long run, this may not be the most
	// appropriate place for this optimization, but it won't cause any harm in
	// the short term.
	connectedPeers := bsnet.host.Network().Peers()
	out := make(chan peer.ID, len(connectedPeers)) // just enough buffer for these connectedPeers
	for _, id := range connectedPeers {
		if id == bsnet.host.ID() {
			continue // ignore self as provider
		}
		out <- id
	}

	go func() {
		defer close(out)
		providers := bsnet.routing.FindProvidersAsync(ctx, k, max)
		for info := range providers {
			if info.ID == bsnet.host.ID() {
				continue // ignore self as provider
			}
			bsnet.host.Peerstore().AddAddrs(info.ID, info.Addrs, pstore.TempAddrTTL)
			select {
			case <-ctx.Done():
				return
			case out <- info.ID:
			}
		}
	}()
	return out
}

// Provide provides the key to the network
func (bsnet *impl) Provide(ctx context.Context, k cid.Cid) error {
	return bsnet.routing.Provide(ctx, k, true)
}

// handleNewStream receives a new stream from the network.
func (bsnet *impl) handleNewStream(s inet.Stream) {
	defer s.Close()

	if bsnet.receiver == nil {
		s.Reset()
		return
	}

	reader := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	for {
		received, err := bsmsg.FromPBReader(reader)
		if err != nil {
			if err != io.EOF {
				s.Reset()
				go bsnet.receiver.ReceiveError(err)
				log.Debugf("bitswap net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("bitswap net handleNewStream from %s", s.Conn().RemotePeer())
		bsnet.receiver.ReceiveMessage(ctx, p, received)
		atomic.AddUint64(&bsnet.stats.MessagesRecvd, 1)
	}
}

func (bsnet *impl) ConnectionManager() ifconnmgr.ConnManager {
	return bsnet.host.ConnManager()
}

func (bsnet *impl) Stats() NetworkStats {
	return NetworkStats{
		MessagesRecvd: atomic.LoadUint64(&bsnet.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&bsnet.stats.MessagesSent),
	}
}

type netNotifiee impl

func (nn *netNotifiee) impl() *impl {
	return (*impl)(nn)
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	nn.impl().receiver.PeerConnected(v.RemotePeer())
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	nn.impl().receiver.PeerDisconnected(v.RemotePeer())
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
