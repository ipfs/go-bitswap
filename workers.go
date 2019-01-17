package bitswap

import (
	"context"
	"math/rand"
	"sync"
	"time"

	engine "github.com/ipfs/go-bitswap/decision"
	bsmsg "github.com/ipfs/go-bitswap/message"
	cid "github.com/ipfs/go-cid"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
	peer "github.com/libp2p/go-libp2p-peer"
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

	// Start up a worker to manage sending out provides messages
	px.Go(func(px process.Process) {
		bs.provideCollector(ctx)
	})

	// Spawn up multiple workers to handle incoming blocks
	// consider increasing number if providing blocks bottlenecks
	// file transfers
	px.Go(bs.provideWorker)
}

func (bs *Bitswap) taskWorker(ctx context.Context, id int) {
	ctx = log.Start(ctx, "Bitswap.Taskworker")
	defer log.Finish(ctx)
	log.SetTag(ctx, "ID", id)
	defer log.Debug("bitswap task worker shutting down...")
	for {
		log.LogKV(ctx, "Bitswap.TaskWorker.Loop", true)
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
					log.LogKV(ctx,
						"Bitswap.TaskWorker.Work", true,
						"Target", envelope.Peer.Pretty(),
						"Block", block.Cid().String(),
					)
					outgoing.AddBlock(block)
				}
				bs.engine.MessageSent(envelope.Peer, outgoing)

				bs.sendBlocks(ctx, envelope)
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

func (bs *Bitswap) sendBlocks(ctx context.Context, env *engine.Envelope) {
	// Blocks need to be sent synchronously to maintain proper backpressure
	// throughout the network stack
	defer env.Sent()

	msgSize := 0
	msg := bsmsg.New(false)
	for _, block := range env.Message.Blocks() {
		msgSize += len(block.RawData())
		msg.AddBlock(block)
		log.Infof("Sending block %s to %s", block, env.Peer)
	}

	bs.sentHistogram.Observe(float64(msgSize))
	err := bs.network.SendMessage(ctx, env.Peer, msg)
	if err != nil {
		log.Infof("sendblock error: %s", err)
	}
}

func (bs *Bitswap) provideWorker(px process.Process) {

	limit := make(chan struct{}, provideWorkerMax)

	limitedGoProvide := func(k cid.Cid, wid int) {
		defer func() {
			// replace token when done
			<-limit
		}()

		ctx := procctx.OnClosingContext(px) // derive ctx from px
		ctx = log.Start(ctx, "Bitswap.ProvideWorker.Work")
		log.SetTag(ctx, "ID", wid)
		log.SetTag(ctx, "cid", k.String())
		defer log.Finish(ctx)
		ctx, cancel := context.WithTimeout(ctx, provideTimeout) // timeout ctx
		defer cancel()

		if err := bs.network.Provide(ctx, k); err != nil {
			log.Warning(err)
		}
	}

	ctx := procctx.OnClosingContext(px)
	ctx = log.Start(ctx, "Bitswap.ProvideWorker")
	defer log.Finish(ctx)

	// worker spawner, reads from bs.provideKeys until it closes, spawning a
	// _ratelimited_ number of workers to handle each key.
	for wid := 2; ; wid++ {
		log.LogKV(ctx, "Bitswap.ProvideWorker.Loop", true, "ID", 1)

		select {
		case <-px.Closing():
			return
		case k, ok := <-bs.provideKeys:
			if !ok {
				log.Debug("provideKeys channel closed")
				return
			}
			select {
			case <-px.Closing():
				return
			case limit <- struct{}{}:
				go limitedGoProvide(k, wid)
			}
		}
	}
}

func (bs *Bitswap) provideCollector(ctx context.Context) {
	defer close(bs.provideKeys)
	var toProvide []cid.Cid
	var nextKey cid.Cid
	var keysOut chan cid.Cid

	for {
		select {
		case blkey, ok := <-bs.newBlocks:
			if !ok {
				log.Debug("newBlocks channel closed")
				return
			}

			if keysOut == nil {
				nextKey = blkey
				keysOut = bs.provideKeys
			} else {
				toProvide = append(toProvide, blkey)
			}
		case keysOut <- nextKey:
			if len(toProvide) > 0 {
				nextKey = toProvide[0]
				toProvide = toProvide[1:]
			} else {
				keysOut = nil
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *Bitswap) rebroadcastWorker(parent context.Context) {
	ctx := log.Start(parent, "Bitswap.Rebroadcast")
	defer log.Finish(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	broadcastSignal := time.NewTicker(rebroadcastDelay.Get())
	defer broadcastSignal.Stop()

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		log.LogKV(ctx, "Bitswap.Rebroadcast.idle", true)
		select {
		case <-tick.C:
			n := bs.wm.WantCount()
			if n > 0 {
				log.Debugf("%d keys in bitswap wantlist", n)
			}
		case <-broadcastSignal.C: // resend unfulfilled wantlist keys
			log.LogKV(ctx, "Bitswap.Rebroadcast.active", true)
			entries := bs.wm.CurrentWants()
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
							log.Debugf("failed to connect to provider %s: %s", p, err)
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
