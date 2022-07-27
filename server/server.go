package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ipfs/go-bitswap/message"
	bitswap_message_pb "github.com/ipfs/go-bitswap/message/pb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
	"go.uber.org/zap"
)

var (
	ProtocolBitswapNoVers  protocol.ID = "/ipfs/bitswap"
	ProtocolBitswapOneZero protocol.ID = "/ipfs/bitswap/1.0.0"
	ProtocolBitswapOneOne  protocol.ID = "/ipfs/bitswap/1.1.0"
	ProtocolBitswap        protocol.ID = "/ipfs/bitswap/1.2.0"
	log                                = logging.Logger("bitswap-server")
	sflog                              = log.Desugar()
)

type server struct {
	Host       host.Host
	Blockstore blockstore.Blockstore

	protocols []protocol.ID
}

type syncMessage struct {
	message.BitSwapMessage
	sync.Mutex
}

func New(host host.Host, bstore blockstore.Blockstore, opts ...func(s *server)) (*server, error) {
	s := &server{
		Host:       host,
		Blockstore: bstore,
		protocols: []protocol.ID{
			ProtocolBitswap,
			ProtocolBitswapNoVers,
			ProtocolBitswapOneOne,
			ProtocolBitswapOneZero,
		},
	}

	for _, o := range opts {
		o(s)
	}

	for _, protocol := range s.protocols {
		s.Host.SetStreamHandler(protocol, s.handleNewStream)
	}

	return s, nil
}

func WithProtocols(protocols []protocol.ID) func(s *server) {
	return func(s *server) {
		s.protocols = protocols
	}
}

func (s *server) processWant(ctx context.Context, peer peer.ID, want message.Entry, resp *syncMessage) {
	blk, err := s.Blockstore.Get(ctx, want.Cid)
	if err != nil && want.SendDontHave {
		resp.Lock()
		resp.AddDontHave(want.Cid)
		resp.Unlock()
		var notFound ipld.ErrNotFound
		if !errors.As(err, &notFound) {
			sflog.Warn("unexpected error reading from blockstore", zap.Stringer("cid", want.Cid), zap.Error(err))
			return
		}
	}

	switch want.WantType {
	case bitswap_message_pb.Message_Wantlist_Have:
		resp.Lock()
		resp.AddHave(want.Cid)
		resp.Unlock()
	case bitswap_message_pb.Message_Wantlist_Block:
		resp.Lock()
		resp.AddBlock(blk)
		resp.Unlock()
	}
}

func (s *server) handleNewStream(stream network.Stream) {
	peer := stream.Conn().RemotePeer()
	sflog.Debug("bitswap server received new stream", zap.Stringer("peer", peer), zap.String("stream", stream.ID()))
	defer func() {
		sflog.Debug("bitswap server closed stream", zap.Stringer("peer", peer), zap.String("stream", stream.ID()))
		stream.Close()
	}()

	reader := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)
	for {
		received, err := message.FromMsgReader(reader)
		if err != nil {
			if err != io.EOF {
				_ = stream.Reset()
				sflog.Debug(
					"bitswap server handleNewStream error",
					zap.Stringer("from", stream.Conn().RemotePeer()),
					zap.Error(err),
				)
			}
			// EOF, stream is closed
			return
		}
		peer := stream.Conn().RemotePeer()
		ctx := context.Background()

		resp := message.New(false)
		syncResp := &syncMessage{BitSwapMessage: resp}
		wg := &sync.WaitGroup{}
		wantList := received.Wantlist()
		wg.Add(len(wantList))
		for _, want := range wantList {
			go func(w message.Entry) {
				s.processWant(ctx, peer, w, syncResp)
				wg.Done()
			}(want)
		}
		wg.Wait()

		err = s.writeMsg(ctx, peer, resp)
		if err != nil {
			sflog.Warn("error sending response", zap.Error(err))
			continue
		}
	}
}

func (s *server) writeMsg(ctx context.Context, peer peer.ID, msg message.BitSwapMessage) error {
	respCtx, respCancel := context.WithTimeout(ctx, time.Second)
	defer respCancel()
	deadline, _ := ctx.Deadline()

	// it is required to open a new stream to send a response
	// TODO: measure impact of not requiring a new stream, to see if we should update the protocol
	respStream, err := s.Host.NewStream(respCtx, peer, s.protocols...)
	if err != nil {
		return fmt.Errorf("opening stream for block response: %w", err)
	}
	defer respStream.Close()

	err = respStream.SetWriteDeadline(deadline)
	if err != nil {
		return fmt.Errorf("setting stream write deadline: %w", err)
	}

	switch respStream.Protocol() {
	case ProtocolBitswapOneOne, ProtocolBitswap:
		err = msg.ToNetV1(respStream)
	case ProtocolBitswapOneZero, ProtocolBitswapNoVers:
		err = msg.ToNetV0(respStream)
	default:
		return fmt.Errorf("unsupported stream protocol %q", respStream.Protocol())
	}

	if err != nil {
		return fmt.Errorf("sending response: %w", err)
	}
	err = respStream.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("resetting response stream deadline: %w", err)
	}
	return nil
}

func (s *server) Close() error {
	return nil
}
