package fuzz

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/logutil"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

const fastSpeed = 60 * time.Millisecond
const mediumSpeed = 200 * time.Millisecond
const slowSpeed = 800 * time.Millisecond
const distribution = 20 * time.Millisecond
const mediumBandwidth = 500000.0
const mediumBandwidthDeviation = 80000.0

func TestCheckLogFileInvariants(t *testing.T) {
	tmpfile := logutil.TeeLogs()

	t.Logf("Writing test log to %s", tmpfile.Name())

	runSwarm(t)

	if err := checkLogFileInvariants(tmpfile.Name()); err != nil {
		t.Fatal(err)
	}
}

func runSwarm(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	numInstances := 10
	numBlocks := 101

	randomGen := rand.New(rand.NewSource(2))

	averageNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.3, 0.3, distribution, randomGen)
	d := delay.Delay(fastSpeed, averageNetworkDelayGenerator)
	rateLimitGenerator := tn.VariableRateLimitGenerator(mediumBandwidth, mediumBandwidthDeviation, randomGen)

	net := tn.RateLimitedVirtualNetwork(mockrouting.NewServer(), d, rateLimitGenerator)
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()
	bg := blocksutil.NewBlockGenerator()

	instances := ig.Instances(numInstances)
	blocks := bg.Blocks(numBlocks)
	var blkeys []cid.Cid
	for _, b := range blocks {
		blkeys = append(blkeys, b.Cid())
	}

	// Put all blocks onto three instances
	for _, seed := range instances[:3] {
		for _, b := range blocks {
			err := seed.Exchange.HasBlock(b)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// Fetch blocks from remaining instances
	wg := sync.WaitGroup{}
	errs := make(chan error)

	for _, inst := range instances[3:] {
		wg.Add(1)
		go func(inst testinstance.Instance) {
			defer wg.Done()

			// Fetch the root block
			outch, err := inst.Exchange.GetBlocks(ctx, blkeys[:1])
			if err != nil {
				errs <- err
			}
			for range outch {
			}

			// Fetch a few more blocks at a time until all blocks have been fetched
			fetched := 1
			for fetched < len(blkeys) {
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

				end := fetched + 5 + rand.Intn(10)
				if end > len(blkeys) {
					end = len(blkeys)
				}

				outch, err := inst.Exchange.GetBlocks(ctx, blkeys[fetched:end])
				if err != nil {
					errs <- err
				}
				for range outch {
					fetched++
				}
			}
		}(inst)
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure all instances got all blocks
	for _, inst := range instances {
		for _, b := range blocks {
			if _, err := inst.Blockstore().Get(b.Cid()); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Give bitswap time to send out cancels before shutting down
	time.Sleep(100 * time.Millisecond)
}
