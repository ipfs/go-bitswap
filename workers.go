package bitswap

import (
	"context"
	"math/rand"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	process "github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p-peer"
)

var TaskWorkerCount = 8

func (bs *Bitswap) startWorkers(px process.Process, ctx context.Context) {
	// Start up a worker to handle block requests this node is making
	px.Go(func(px process.Process) {
		bs.providerQueryManager(ctx)
	})

	// Start up workers to handle requests from other nodes for the data on this node
	for i := 0; i < TaskWorkerCount; i++ {
		i := i
		px.Go(func(px process.Process) {
			bs.taskWorker(ctx, i)
		})
	}

	// Start up a worker to manage periodically resending our wantlist out to peers
	px.Go(func(px process.Process) {
		bs.rebroadcastWorker(ctx)
	})
}

func (bs *Bitswap) taskWorker(ctx context.Context, id int) {
	idmap := logging.LoggableMap{"ID": id}
	defer log.Debug("bitswap task worker shutting down...")
	for {
		log.Event(ctx, "Bitswap.TaskWorker.Loop", idmap)
		select {
		case nextEnvelope := <-bs.engine.Outbox():
			select {
			case envelope, ok := <-nextEnvelope:
				if !ok {
					continue
				}
				// update the BS ledger to reflect sent message
				// TODO: Should only track *useful* messages in ledger
				outgoing := bsmsg.New(false)
				for _, block := range envelope.Message.Blocks() {
					log.Event(ctx, "Bitswap.TaskWorker.Work", logging.LoggableF(func() map[string]interface{} {
						return logging.LoggableMap{
							"ID":     id,
							"Target": envelope.Peer.Pretty(),
							"Block":  block.Cid().String(),
						}
					}))
					outgoing.AddBlock(block)
				}
				bs.engine.MessageSent(envelope.Peer, outgoing)

				bs.wm.SendBlocks(ctx, envelope)
				bs.counterLk.Lock()
				for _, block := range envelope.Message.Blocks() {
					bs.counters.blocksSent++
					bs.counters.dataSent += uint64(len(block.RawData()))
				}
				bs.counterLk.Unlock()
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *Bitswap) rebroadcastWorker(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	broadcastSignal := time.NewTicker(rebroadcastDelay.Get())
	defer broadcastSignal.Stop()

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		log.Event(ctx, "Bitswap.Rebroadcast.idle")
		select {
		case <-tick.C:
			n := bs.wm.wl.Len()
			if n > 0 {
				log.Debug(n, " keys in bitswap wantlist")
			}
		case <-broadcastSignal.C: // resend unfulfilled wantlist keys
			log.Event(ctx, "Bitswap.Rebroadcast.active")
			entries := bs.wm.wl.Entries()
			if len(entries) == 0 {
				continue
			}

			// TODO: come up with a better strategy for determining when to search
			// for new providers for blocks.
			i := rand.Intn(len(entries))
			bs.findKeys <- &blockRequest{
				Cid: entries[i].Cid,
				Ctx: ctx,
			}
		case <-parent.Done():
			return
		}
	}
}

func (bs *Bitswap) providerQueryManager(ctx context.Context) {
	var activeLk sync.Mutex
	kset := cid.NewSet()

	for {
		select {
		case e := <-bs.findKeys:
			select { // make sure its not already cancelled
			case <-e.Ctx.Done():
				continue
			default:
			}

			activeLk.Lock()
			if kset.Has(e.Cid) {
				activeLk.Unlock()
				continue
			}
			kset.Add(e.Cid)
			activeLk.Unlock()

			go func(e *blockRequest) {
				child, cancel := context.WithTimeout(e.Ctx, providerRequestTimeout)
				defer cancel()
				providers := bs.network.FindProvidersAsync(child, e.Cid, maxProvidersPerRequest)
				wg := &sync.WaitGroup{}
				for p := range providers {
					wg.Add(1)
					go func(p peer.ID) {
						defer wg.Done()
						err := bs.network.ConnectTo(child, p)
						if err != nil {
							log.Debug("failed to connect to provider %s: %s", p, err)
						}
					}(p)
				}
				wg.Wait()
				activeLk.Lock()
				kset.Remove(e.Cid)
				activeLk.Unlock()
			}(e)

		case <-ctx.Done():
			return
		}
	}
}
