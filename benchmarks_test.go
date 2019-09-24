package bitswap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"
	blocks "github.com/ipfs/go-block-format"

	bitswap "github.com/ipfs/go-bitswap"
	bssession "github.com/ipfs/go-bitswap/session"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("bs:bnch")

type fetchFunc func(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid)

type distFunc func(b *testing.B, provs []testinstance.Instance, blocks []blocks.Block)

type runStats struct {
	Dups    uint64
	MsgSent uint64
	MsgRecd uint64
	Time    time.Duration
	Name    string
}

var benchmarkLog []runStats

type bench struct {
	name       string
	nodeCount  int
	blockCount int
	distFn     distFunc
	fetchFn    fetchFunc
}

var benches = []bench{
	// Fetch from two seed nodes that both have all 100 blocks
	// - request one at a time, in series
	bench{"3Nodes-AllToAll-OneAtATime", 3, 100, allToAll, oneAtATime},
	// - request all 100 with a single GetBlocks() call
	bench{"3Nodes-AllToAll-BigBatch", 3, 100, allToAll, batchFetchAll},

	// Fetch from two seed nodes, one at a time, where:
	// - node A has blocks 0 - 74
	// - node B has blocks 25 - 99
	// * Slow because we only get blocks from one node for 75 blocks, then
	//   have to broadcast
	bench{"3Nodes-Overlap1-OneAtATime", 3, 100, overlap1, oneAtATime},

	// Fetch from two seed nodes, where:
	// - node A has even blocks
	// - node B has odd blocks
	// - both nodes have every third block

	// - request one at a time, in series
	// * Slow because when potential-threshold decreases to 0.5,
	//   we frequently have to send want-have then wait for HAVE then
	//   send want-block
	bench{"3Nodes-Overlap3-OneAtATime", 3, 100, overlap2, oneAtATime},
	// - request 10 at a time, in series
	bench{"3Nodes-Overlap3-BatchBy10", 3, 100, overlap2, batchFetchBy10},
	// - request all 100 in parallel as individual GetBlock() calls
	bench{"3Nodes-Overlap3-AllConcurrent", 3, 100, overlap2, fetchAllConcurrent},
	// - request all 100 with a single GetBlocks() call
	bench{"3Nodes-Overlap3-BigBatch", 3, 100, overlap2, batchFetchAll},
	// - request 1, then 10, then 89 blocks (similar to how IPFS would fetch a file)
	bench{"3Nodes-Overlap3-UnixfsFetch", 3, 100, overlap2, unixfsFileFetch},

	// Fetch from nine seed nodes, all nodes have all blocks
	// - request one at a time, in series
	bench{"10Nodes-AllToAll-OneAtATime", 10, 100, allToAll, oneAtATime},
	// - request 10 at a time, in series
	bench{"10Nodes-AllToAll-BatchFetchBy10", 10, 100, allToAll, batchFetchBy10},
	// - request all 100 with a single GetBlocks() call
	bench{"10Nodes-AllToAll-BigBatch", 10, 100, allToAll, batchFetchAll},
	// - request all 100 in parallel as individual GetBlock() calls
	bench{"10Nodes-AllToAll-AllConcurrent", 10, 100, allToAll, fetchAllConcurrent},
	// - request 1, then 10, then 89 blocks (similar to how IPFS would fetch a file)
	bench{"10Nodes-AllToAll-UnixfsFetch", 10, 100, allToAll, unixfsFileFetch},

	// Fetch from nine seed nodes, blocks are distributed randomly across all nodes (no dups)
	// - request one at a time, in series
	// * Slow because
	//   1. We broadcast to 9 nodes, get 1 response (1 session peer)
	//   2. Then ask that peer for next block
	//   3. Times out
	//   4. Repeat 1, (2 session peers)
	//   Possible optimization: if all peers in the session send DONT_HAVEs for
	//   block, broadcast to all connected peers (check if there are more connected
	//   peers than session peers, and somehow limit broadcasts to make sure they're
	//   not too frequent)
	bench{"10Nodes-OnePeerPerBlock-OneAtATime", 10, 100, onePeerPerBlock, oneAtATime},
	// - request all 100 with a single GetBlocks() call
	bench{"10Nodes-OnePeerPerBlock-BigBatch", 10, 100, onePeerPerBlock, batchFetchAll},
	// - request 1, then 10, then 89 blocks (similar to how IPFS would fetch a file)
	bench{"10Nodes-OnePeerPerBlock-UnixfsFetch", 10, 100, onePeerPerBlock, unixfsFileFetch},

	// Fetch from 19 seed nodes, all nodes have all blocks, fetch all 200 blocks with a single GetBlocks() call
	bench{"200Nodes-AllToAll-BigBatch", 200, 20, allToAll, batchFetchAll},
}

func BenchmarkFixedDelay(b *testing.B) {
	benchmarkLog = nil
	fixedDelay := delay.Fixed(10 * time.Millisecond)
	bstoreLatency := time.Duration(0)

	for _, bch := range benches {
		b.Run(bch.name, func(b *testing.B) {
			subtestDistributeAndFetch(b, bch.nodeCount, bch.blockCount, fixedDelay, bstoreLatency, bch.distFn, bch.fetchFn)
		})
	}

	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

const datacenterSpeed = 5 * time.Millisecond
const fastSpeed = 60 * time.Millisecond
const mediumSpeed = 200 * time.Millisecond
const slowSpeed = 800 * time.Millisecond
const superSlowSpeed = 4000 * time.Millisecond
const datacenterDistribution = 3 * time.Millisecond
const distribution = 20 * time.Millisecond
const datacenterBandwidth = 125000000.0
const datacenterBandwidthDeviation = 3000000.0
const fastBandwidth = 1250000.0
const fastBandwidthDeviation = 300000.0
const mediumBandwidth = 500000.0
const mediumBandwidthDeviation = 80000.0
const slowBandwidth = 100000.0
const slowBandwidthDeviation = 16500.0
const stdBlockSize = 8000
const largeBlockSize = 256 * 1024

func BenchmarkRealWorld(b *testing.B) {
	benchmarkLog = nil
	benchmarkSeed, err := strconv.ParseInt(os.Getenv("BENCHMARK_SEED"), 10, 64)
	var randomGen *rand.Rand = nil
	if err == nil {
		randomGen = rand.New(rand.NewSource(benchmarkSeed))
	}

	fastNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.0, 0.0, distribution, randomGen)
	fastNetworkDelay := delay.Delay(fastSpeed, fastNetworkDelayGenerator)
	fastBandwidthGenerator := tn.VariableRateLimitGenerator(fastBandwidth, fastBandwidthDeviation, randomGen)
	averageNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.3, 0.3, distribution, randomGen)
	averageNetworkDelay := delay.Delay(fastSpeed, averageNetworkDelayGenerator)
	averageBandwidthGenerator := tn.VariableRateLimitGenerator(mediumBandwidth, mediumBandwidthDeviation, randomGen)
	slowNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, superSlowSpeed-fastSpeed,
		0.3, 0.3, distribution, randomGen)
	slowNetworkDelay := delay.Delay(fastSpeed, slowNetworkDelayGenerator)
	slowBandwidthGenerator := tn.VariableRateLimitGenerator(slowBandwidth, slowBandwidthDeviation, randomGen)
	bstoreLatency := time.Duration(0)

	b.Run("200Nodes-AllToAll-BigBatch-FastNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, fastNetworkDelay, fastBandwidthGenerator, stdBlockSize, bstoreLatency, allToAll, batchFetchAll)
	})
	b.Run("200Nodes-AllToAll-BigBatch-AverageVariableSpeedNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, averageNetworkDelay, averageBandwidthGenerator, stdBlockSize, bstoreLatency, allToAll, batchFetchAll)
	})
	b.Run("200Nodes-AllToAll-BigBatch-SlowVariableSpeedNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, slowNetworkDelay, slowBandwidthGenerator, stdBlockSize, bstoreLatency, allToAll, batchFetchAll)
	})
	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/rw-benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

func BenchmarkDatacenter(b *testing.B) {
	benchmarkLog = nil
	benchmarkSeed, err := strconv.ParseInt(os.Getenv("BENCHMARK_SEED"), 10, 64)
	var randomGen *rand.Rand = nil
	if err == nil {
		randomGen = rand.New(rand.NewSource(benchmarkSeed))
	}

	datacenterNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		fastSpeed-datacenterSpeed, (fastSpeed-datacenterSpeed) / 2,
		0.0, 0.0, datacenterDistribution, randomGen)
	datacenterNetworkDelay := delay.Delay(datacenterSpeed, datacenterNetworkDelayGenerator)
	datacenterBandwidthGenerator := tn.VariableRateLimitGenerator(datacenterBandwidth, datacenterBandwidthDeviation, randomGen)
	bstoreLatency := time.Millisecond * 25

	b.Run("3Nodes-Overlap3-UnixfsFetch", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 3, 100, datacenterNetworkDelay, datacenterBandwidthGenerator, largeBlockSize, bstoreLatency, allToAll, unixfsFileFetch)
	})
	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/rb-benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

func subtestDistributeAndFetch(b *testing.B, numnodes, numblks int, d delay.D, bstoreLatency time.Duration, df distFunc, ff fetchFunc) {
	for i := 0; i < b.N; i++ {
		// fmt.Println("\n\n\n\nStarting bench")
		start := time.Now()
		net := tn.VirtualNetwork(mockrouting.NewServer(), d)

		ig := testinstance.NewTestInstanceGenerator(net)

		bg := blocksutil.NewBlockGenerator()

		instances := ig.Instances(numnodes)
		blocks := bg.Blocks(numblks)
		runDistribution(b, instances, blocks, bstoreLatency, df, ff, start)
		ig.Close()
	}
}

func subtestDistributeAndFetchRateLimited(b *testing.B, numnodes, numblks int, d delay.D, rateLimitGenerator tn.RateLimitGenerator, blockSize int64, bstoreLatency time.Duration, df distFunc, ff fetchFunc) {
	for i := 0; i < b.N; i++ {
		start := time.Now()
		net := tn.RateLimitedVirtualNetwork(mockrouting.NewServer(), d, rateLimitGenerator)

		ig := testinstance.NewTestInstanceGenerator(net)
		defer ig.Close()

		instances := ig.Instances(numnodes)
		blocks := testutil.GenerateBlocksOfSize(numblks, blockSize)
		runDistribution(b, instances, blocks, bstoreLatency, df, ff, start)
	}
}

func runDistribution(b *testing.B, instances []testinstance.Instance, blocks []blocks.Block, bstoreLatency time.Duration, df distFunc, ff fetchFunc, start time.Time) {
	numnodes := len(instances)
	fetcher := instances[numnodes-1]

	// Distribute blocks to seed nodes
	seeds := instances[:numnodes-1]
	df(b, seeds, blocks)
	
	// Set the blockstore latency on seed nodes
	if bstoreLatency > 0 {
		for _, i := range seeds {
			i.SetBlockstoreLatency(bstoreLatency)
		}
	}

	// Fetch blocks (from seed nodes to leech nodes)
	var ks []cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}
	ff(b, fetcher.Exchange, ks)

	// Collect statistics
	st, err := fetcher.Exchange.Stat()
	if err != nil {
		b.Fatal(err)
	}

	nst := fetcher.Adapter.Stats()
	stats := runStats{
		Time:    time.Since(start),
		MsgRecd: nst.MessagesRecvd,
		MsgSent: nst.MessagesSent,
		Dups:    st.DupBlksReceived,
		Name:    b.Name(),
	}
	benchmarkLog = append(benchmarkLog, stats)
	// b.Logf("send/recv: %d / %d (dups: %d)", nst.MessagesSent, nst.MessagesRecvd, st.DupBlksReceived)
}

func allToAll(b *testing.B, provs []testinstance.Instance, blocks []blocks.Block) {
	for _, p := range provs {
		if err := p.Blockstore().PutMany(blocks); err != nil {
			b.Fatal(err)
		}
	}
}

// overlap1 gives the first 75 blocks to the first peer, and the last 75 blocks
// to the second peer. This means both peers have the middle 50 blocks
func overlap1(b *testing.B, provs []testinstance.Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		b.Fatal("overlap1 only works with 2 provs")
	}
	bill := provs[0]
	jeff := provs[1]

	if err := bill.Blockstore().PutMany(blks[:75]); err != nil {
		b.Fatal(err)
	}
	if err := jeff.Blockstore().PutMany(blks[25:]); err != nil {
		b.Fatal(err)
	}
}

// overlap2 gives every even numbered block to the first peer, odd numbered
// blocks to the second.  it also gives every third block to both peers
func overlap2(b *testing.B, provs []testinstance.Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		b.Fatal("overlap2 only works with 2 provs")
	}
	bill := provs[0]
	jeff := provs[1]

	for i, blk := range blks {
		even := i%2 == 0
		third := i%3 == 0
		if third || even {
			if err := bill.Blockstore().Put(blk); err != nil {
				b.Fatal(err)
			}
		}
		if third || !even {
			if err := jeff.Blockstore().Put(blk); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// onePeerPerBlock picks a random peer to hold each block
// with this layout, we shouldnt actually ever see any duplicate blocks
// but we're mostly just testing performance of the sync algorithm
func onePeerPerBlock(b *testing.B, provs []testinstance.Instance, blks []blocks.Block) {
	for _, blk := range blks {
		err := provs[rand.Intn(len(provs))].Blockstore().Put(blk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func oneAtATime(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background()).(*bssession.Session)
	for _, c := range ks {
		_, err := ses.GetBlock(context.Background(), c)
		if err != nil {
			b.Fatal(err)
		}
	}
	// b.Logf("Session fetch latency: %s", ses.GetAverageLatency())
}

// fetch data in batches, 10 at a time
func batchFetchBy10(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	for i := 0; i < len(ks); i += 10 {
		out, err := ses.GetBlocks(context.Background(), ks[i:i+10])
		if err != nil {
			b.Fatal(err)
		}
		for range out {
		}
	}
}

// fetch each block at the same time concurrently
func fetchAllConcurrent(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())

	var wg sync.WaitGroup
	for _, c := range ks {
		wg.Add(1)
		go func(c cid.Cid) {
			defer wg.Done()
			_, err := ses.GetBlock(context.Background(), c)
			if err != nil {
				b.Error(err)
			}
		}(c)
	}
	wg.Wait()
}

func batchFetchAll(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	out, err := ses.GetBlocks(context.Background(), ks)
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}
}

// simulates the fetch pattern of trying to sync a unixfs file graph as fast as possible
func unixfsFileFetch(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	_, err := ses.GetBlock(context.Background(), ks[0])
	if err != nil {
		b.Fatal(err)
	}

	out, err := ses.GetBlocks(context.Background(), ks[1:11])
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}

	out, err = ses.GetBlocks(context.Background(), ks[11:])
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}
}

func printResults(rs []runStats) {
	nameOrder := make([]string, 0)
	names := make(map[string]struct{})
	for i := 0; i < len(rs); i++ {
		if _, ok := names[rs[i].Name]; !ok {
			nameOrder = append(nameOrder, rs[i].Name)
			names[rs[i].Name] = struct{}{}
		}
	}

	for i := 0; i < len(names); i++ {
		name := nameOrder[i]
		count := 0
		sent := 0.0
		rcvd := 0.0
		dups := 0.0
		elpd := 0.0
		for i := 0; i < len(rs); i++ {
			if rs[i].Name == name {
				count++
				sent += float64(rs[i].MsgSent)
				rcvd += float64(rs[i].MsgRecd)
				dups += float64(rs[i].Dups)
				elpd += float64(rs[i].Time)
			}
		}
		sent /= float64(count)
		rcvd /= float64(count)
		dups /= float64(count)

		label := fmt.Sprintf("%s (%d runs / %.2fs):", name, count, elpd/1000000000.0)
		fmt.Printf("%-75s %s / sent %d / recv %d / dups %d\n",
			label,
			fmtDuration(time.Duration(int64(math.Round(elpd/float64(count))))),
			int64(math.Round(sent)), int64(math.Round(rcvd)), int64(math.Round(dups)))
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	s := d / time.Second
	d -= s * time.Second
	ms := d / time.Millisecond
	return fmt.Sprintf("%d.%03ds", s, ms)
}
