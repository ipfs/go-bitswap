package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	bitswap "github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/client"
	bsnet "github.com/ipfs/go-bitswap/network"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dstoresync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

func main() {
	blockSizeMiBFlag := flag.Int("blocksize", 1, "block size in MiB")
	blockCountFlag := flag.Int("blocks", 1000, "number of blocks")
	concReqsFlag := flag.Int("conc", 10, "max concurrent requests")
	provsFlag := flag.Int("provs", 1, "number of providers")
	clientFlag := flag.Bool("client", false, "use the new client")
	flag.Parse()

	ctx := context.Background()

	provider := makeProvider(ctx, *provsFlag)
	requester := makeRequester(ctx, *clientFlag)

	err := requester.ConnectTo(ctx, provider.AddrInfo())
	if err != nil {
		log.Panic(err)
	}

	blockSize := *blockSizeMiBFlag * 1024 * 1024

	mhs, err := provider.PublishBlocks(ctx, blockSize, *blockCountFlag)
	if err != nil {
		log.Panic(err)
	}

	err = requester.RequestBlocks(ctx, *concReqsFlag, mhs)
	if err != nil {
		log.Panic(err.Error())
	}
}

func makeProvider(ctx context.Context, numProviders int) *provider {
	listen, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3333")
	if err != nil {
		log.Panic(err.Error())
	}
	h, err := libp2p.New(ctx, libp2p.ListenAddrs(listen))
	if err != nil {
		log.Panic(err)
	}
	kad, err := dht.New(ctx, h)
	if err != nil {
		log.Panic(err)
	}
	for _, a := range h.Addrs() {
		log.Printf("provider listening on addr: %s", a)
	}
	bstore := blockstore.NewBlockstore(dstoresync.MutexWrap(datastore.NewMapDatastore()))
	ex := bitswap.New(ctx, bsnet.NewFromIpfsHost(h, kad), bstore)
	return &provider{
		host:     h,
		bstore:   bstore,
		exchange: ex,
	}
}

func makeRequester(ctx context.Context, useNewClient bool) *requester {
	listen, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3334")
	if err != nil {
		log.Panic(err)
	}
	h, err := libp2p.New(ctx, libp2p.ListenAddrs(listen))
	if err != nil {
		log.Panic(err)
	}
	kad, err := dht.New(ctx, h)
	if err != nil {
		log.Panic(err)
	}
	for _, a := range h.Addrs() {
		log.Printf("requester listening on addr: %s", a.String())
	}
	bstore := blockstore.NewBlockstore(dstoresync.MutexWrap(datastore.NewMapDatastore()))

	var fetcher exchange.Fetcher
	if useNewClient {
		log.Printf("using new client")
		fetcher = client.New(kad.ProviderStore(), h, bstore)
	} else {
		fetcher = bitswap.New(ctx, bsnet.NewFromIpfsHost(h, kad), bstore)
	}

	return &requester{
		host:    h,
		bstore:  bstore,
		fetcher: fetcher,
	}
}

type provider struct {
	host     host.Host
	bstore   blockstore.Blockstore
	exchange exchange.Interface
}

func (p *provider) AddrInfo() peer.AddrInfo {
	return peer.AddrInfo{
		ID:    p.host.ID(),
		Addrs: p.host.Addrs(),
	}
}

type makeBlockResult struct {
	mh  multihash.Multihash
	err error
}

func (p *provider) PublishBlocks(ctx context.Context, blockSize int, blockCount int) ([]multihash.Multihash, error) {
	log.Printf("generating %d blocks of %d bytes", blockCount, blockSize)
	wg := sync.WaitGroup{}
	workChan := make(chan struct{})
	resultChan := make(chan makeBlockResult)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range workChan {
				buf := make([]byte, blockSize)
				rand.Read(buf)
				blk := block.NewBlock(buf)
				err := p.bstore.Put(ctx, blk)
				if err != nil {
					resultChan <- makeBlockResult{err: err}
					return
				}
				err = p.exchange.HasBlock(ctx, blk)
				if err != nil {
					resultChan <- makeBlockResult{err: err}
					return
				}
				resultChan <- makeBlockResult{mh: blk.Multihash()}
			}
		}()
	}
	go func() {
		for i := 0; i < blockCount; i++ {
			workChan <- struct{}{}
		}
		close(workChan)
		wg.Wait()
		close(resultChan)
	}()

	mhs := []multihash.Multihash{}
	for result := range resultChan {
		if result.err != nil {
			return nil, result.err
		}
		mhs = append(mhs, result.mh)
	}

	return mhs, nil
}

type requester struct {
	host    host.Host
	bstore  blockstore.Blockstore
	fetcher exchange.Fetcher
}

func (r *requester) AddrInfo() peer.AddrInfo {
	return peer.AddrInfo{
		ID:    r.host.ID(),
		Addrs: r.host.Addrs(),
	}
}

func (r *requester) ConnectTo(ctx context.Context, ai peer.AddrInfo) error {
	log.Printf("connecting to provider: %s", ai)
	err := r.host.Connect(ctx, ai)
	if err != nil {
		return fmt.Errorf("could not connect to provider: %w", err)
	}
	log.Printf("connected to %s", ai)
	return nil
}

type result struct {
	mh       multihash.Multihash
	err      error
	duration time.Duration
}

func (r *requester) RequestBlocks(ctx context.Context, workers int, mhs []multihash.Multihash) error {
	dls := []time.Duration{}

	mhChan := make(chan multihash.Multihash)
	resultChan := make(chan result)
	wg := sync.WaitGroup{}

	log.Printf("requesting %d blocks with %d workers", len(mhs), workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for mh := range mhChan {
				downloadStart := time.Now()
				blk, err := r.fetcher.GetBlock(ctx, cid.NewCidV0(mh))
				if err != nil {
					resultChan <- result{mh: mh, err: fmt.Errorf("error fetching %s: %w", mh, err)}
					return
				}

				err = r.bstore.Put(ctx, blk)
				if err != nil {
					resultChan <- result{mh: mh, err: fmt.Errorf("error putting block into blockstore %s: %w", mh, err)}
					return
				}
				resultChan <- result{mh: mh, duration: time.Since(downloadStart)}
			}
		}()
	}

	go func() {
		for _, mh := range mhs {
			mhChan <- mh
		}
		close(mhChan)
		wg.Wait()
		close(resultChan)
	}()

	start := time.Now()

	for result := range resultChan {
		if result.err != nil {
			return result.err
		}
		dls = append(dls, result.duration)
	}

	duration := time.Since(start)

	log.Printf("Requested %d blocks in %d ms", len(mhs), duration.Milliseconds())
	sort.Slice(dls, func(i, j int) bool { return dls[i].Nanoseconds() < dls[j].Nanoseconds() })

	count := len(mhs)
	sum := 0
	for _, d := range dls {
		sum += int(d.Milliseconds())
	}
	log.Printf("Min: %d ms", dls[0].Milliseconds())
	log.Printf("Max: %d ms", dls[len(dls)-1].Milliseconds())
	log.Printf("Median: %d ms", dls[len(dls)/2].Milliseconds())
	log.Printf("Avg: %d ms", sum/count)

	return nil
}
