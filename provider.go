package bitswap

import (
	"context"
	"sync"

	"github.com/ipfs/go-bitswap/internal/defaults"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var (
	// HasBlockBufferSize is the buffer size of the channel for new blocks
	// that need to be provided. They should get pulled over by the
	// provideCollector even before they are actually provided.
	// TODO: Does this need to be this large givent that?
	HasBlockBufferSize    = 256
	provideKeysBufferSize = 2048
	provideWorkerMax      = 6
)

type provider struct {
	provider    func(context.Context, cid.Cid) error
	provideKeys chan cid.Cid
	newBlocks   chan cid.Cid
	done        chan struct{}
	closedWG    sync.WaitGroup
}

func newProvider() *provider {
	p := &provider{
		newBlocks:   make(chan cid.Cid, HasBlockBufferSize),
		provideKeys: make(chan cid.Cid, provideKeysBufferSize),
		done:        make(chan struct{}),
	}

	p.closedWG.Add(2)
	go p.provideCollector()
	go p.provideWorker()

	return p
}

func (p *provider) Provide(ctx context.Context, blks []blocks.Block) error {
	if p == nil {
		return nil
	}
	for _, blk := range blks {
		select {
		case p.newBlocks <- blk.Cid():
			// send block off to be reprovided
		case <-p.done:
			return nil
		case <-ctx.Done():
			ctx.Err()
		}
	}

	return nil
}

func (p *provider) Shutdown() {
	if p == nil {
		return
	}
	close(p.done)
	p.closedWG.Wait()
}

func (p *provider) NewBlocksLen() int {
	if p == nil {
		return 0
	}
	return len(p.newBlocks)
}

func (p *provider) provideCollector() {
	var toProvide []cid.Cid
	var nextKey cid.Cid
	var keysOut chan cid.Cid

	for {
		select {
		case blkey, ok := <-p.newBlocks:
			if !ok {
				log.Debug("newBlocks channel closed")
				return
			}

			if keysOut == nil {
				nextKey = blkey
				keysOut = p.provideKeys
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
		case <-p.done:
			return
		}
	}
}

func (p *provider) provideWorker() {
	defer p.closedWG.Done()
	limit := make(chan struct{}, provideWorkerMax)

	limitedGoProvide := func(k cid.Cid, wid int) {
		defer func() {
			// replace token when done
			<-limit
		}()

		log.Debugw("Bitswap.ProvideWorker.Start", "ID", wid, "cid", k)
		defer log.Debugw("Bitswap.ProvideWorker.End", "ID", wid, "cid", k)

		ctx, cancel := context.WithTimeout(context.Background(), defaults.ProvideTimeout)
		defer cancel()

		if err := p.provider(ctx, k); err != nil {
			log.Warn(err)
		}
	}

	// worker spawner, reads from bs.provideKeys until it closes, spawning a
	// _ratelimited_ number of workers to handle each key.
	for wid := 2; ; wid++ {
		log.Debug("Bitswap.ProvideWorker.Loop")

		select {
		case <-p.done:
			return
		case k := <-p.provideKeys:
			select {
			case <-p.done:
				return
			case limit <- struct{}{}:
				go limitedGoProvide(k, wid)
			}
		}
	}
}
