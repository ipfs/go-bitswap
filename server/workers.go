package server

import (
	"context"
	"fmt"
	"time"

	engine "github.com/ipfs/go-bitswap/internal/decision"
	pb "github.com/ipfs/go-bitswap/message/pb"
	process "github.com/jbenet/goprocess"
	"go.uber.org/zap"
)

func (bs *Bitswap) startWorkers(ctx context.Context, px process.Process) {

	// Start up workers to handle requests from other nodes for the data on this node
	for i := 0; i < bs.taskWorkerCount; i++ {
		i := i
		px.Go(func(px process.Process) {
			bs.taskWorker(ctx, i)
		})
	}
}

func (bs *Bitswap) taskWorker(ctx context.Context, id int) {
	defer log.Debug("bitswap task worker shutting down...")
	log := log.With("ID", id)
	for {
		log.Debug("Bitswap.TaskWorker.Loop")
		select {
		case nextEnvelope := <-bs.engine.Outbox():
			select {
			case envelope, ok := <-nextEnvelope:
				if !ok {
					continue
				}

				start := time.Now()

				// TODO: Only record message as sent if there was no error?
				// Ideally, yes. But we'd need some way to trigger a retry and/or drop
				// the peer.
				bs.engine.MessageSent(envelope.Peer, envelope.Message)
				if bs.wiretap != nil {
					bs.wiretap.MessageSent(envelope.Peer, envelope.Message)
				}
				bs.sendBlocks(ctx, envelope)

				dur := time.Since(start)
				bs.sendTimeHistogram.Observe(dur.Seconds())

			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *Bitswap) logOutgoingBlocks(env *engine.Envelope) {
	if ce := sflog.Check(zap.DebugLevel, "sent message"); ce == nil {
		return
	}

	self := bs.network.Self()

	for _, blockPresence := range env.Message.BlockPresences() {
		c := blockPresence.Cid
		switch blockPresence.Type {
		case pb.Message_Have:
			log.Debugw("sent message",
				"type", "HAVE",
				"cid", c,
				"local", self,
				"to", env.Peer,
			)
		case pb.Message_DontHave:
			log.Debugw("sent message",
				"type", "DONT_HAVE",
				"cid", c,
				"local", self,
				"to", env.Peer,
			)
		default:
			panic(fmt.Sprintf("unrecognized BlockPresence type %v", blockPresence.Type))
		}

	}
	for _, block := range env.Message.Blocks() {
		log.Debugw("sent message",
			"type", "BLOCK",
			"cid", block.Cid(),
			"local", self,
			"to", env.Peer,
		)
	}
}

func (bs *Bitswap) sendBlocks(ctx context.Context, env *engine.Envelope) {
	// Blocks need to be sent synchronously to maintain proper backpressure
	// throughout the network stack
	defer env.Sent()

	err := bs.network.SendMessage(ctx, env.Peer, env.Message)
	if err != nil {
		log.Debugw("failed to send blocks message",
			"peer", env.Peer,
			"error", err,
		)
		return
	}

	bs.logOutgoingBlocks(env)

	dataSent := 0
	blocks := env.Message.Blocks()
	for _, b := range blocks {
		dataSent += len(b.RawData())
	}
	bs.counterLk.Lock()
	bs.counters.blocksSent += uint64(len(blocks))
	bs.counters.dataSent += uint64(dataSent)
	bs.counterLk.Unlock()
	bs.sentHistogram.Observe(float64(env.Message.Size()))
	log.Debugw("sent message", "peer", env.Peer)
}
