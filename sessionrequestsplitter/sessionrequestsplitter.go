package sessionrequestsplitter

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	bssd "github.com/ipfs/go-bitswap/sessiondata"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-peer"
)

const (
	minReceivedToAdjustSplit = 2
	maxSplit                 = 16
	minDuplesToTryLessSplits = 0.1
	maxAcceptableDupes       = 0.4
	throughputSampleTime     = 10000 * time.Millisecond
	initialSplit             = 2
)

type srsMessage interface {
	handle(srs *SessionRequestSplitter)
}

// SessionRequestSplitter track how many duplicate and unique blocks come in and
// uses that to determine how much to split up each set of wants among peers.
type SessionRequestSplitter struct {
	ctx      context.Context
	messages chan srsMessage

	// data, do not touch outside run loop
	receivedCount          int
	lastReceivedCount      int
	lastThroughput         int
	splitTestDirection     int
	split                  int
	duplicateReceivedCount int
}

// New returns a new SessionRequestSplitter.
func New(ctx context.Context) *SessionRequestSplitter {
	srs := &SessionRequestSplitter{
		ctx:                ctx,
		messages:           make(chan srsMessage, 10),
		split:              initialSplit,
		splitTestDirection: 1,
	}
	go srs.run()
	return srs
}

// SplitRequest splits a request for the given cids one or more times among the
// given peers.
func (srs *SessionRequestSplitter) SplitRequest(optimizedPeers []bssd.OptimizedPeer, ks []cid.Cid) []bssd.PartialRequest {
	resp := make(chan []bssd.PartialRequest, 1)

	select {
	case srs.messages <- &splitRequestMessage{optimizedPeers, ks, resp}:
	case <-srs.ctx.Done():
		return nil
	}
	select {
	case splitRequests := <-resp:
		return splitRequests
	case <-srs.ctx.Done():
		return nil
	}

}

// RecordDuplicateBlock records the fact that the session received a duplicate
// block and adjusts split factor as neccesary.
func (srs *SessionRequestSplitter) RecordDuplicateBlock() {
	select {
	case srs.messages <- &recordDuplicateMessage{}:
	case <-srs.ctx.Done():
	}
}

// RecordUniqueBlock records the fact that the session received unique block
// and adjusts the split factor as neccesary.
func (srs *SessionRequestSplitter) RecordUniqueBlock() {
	select {
	case srs.messages <- &recordUniqueMessage{}:
	case <-srs.ctx.Done():
	}
}

func (srs *SessionRequestSplitter) run() {
	timer := time.NewTicker(throughputSampleTime)
	for {
		select {
		case <-timer.C:
			srs.handleThroughputSample()
		case message := <-srs.messages:
			message.handle(srs)
		case <-srs.ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (srs *SessionRequestSplitter) handleThroughputSample() {
	throughput := srs.receivedCount - srs.lastReceivedCount
	srs.lastReceivedCount = srs.receivedCount
	if srs.lastThroughput != 0 {
		if throughput < srs.lastThroughput {
			srs.splitTestDirection = -1 * srs.splitTestDirection
		}
		nextSplit := srs.split + srs.splitTestDirection
		if (nextSplit >= 1) && (nextSplit <= maxSplit) {
			srs.split = nextSplit
			fmt.Println(srs.split)
		}
	}
	srs.lastThroughput = throughput
}

func (srs *SessionRequestSplitter) duplicateRatio() float64 {
	return float64(srs.duplicateReceivedCount) / float64(srs.receivedCount)
}

func transformOptimization(optimizationRating float64) float64 {
	return optimizationRating * optimizationRating * optimizationRating * optimizationRating * optimizationRating
}

func (srs *SessionRequestSplitter) peersFromOptimizedPeers(optimizedPeers []bssd.OptimizedPeer) []peer.ID {
	optimizationTotal := 0.0
	skipMap := make([]int, len(optimizedPeers))
	for i, optimizedPeer := range optimizedPeers {
		optimizationTotal += transformOptimization(optimizedPeer.OptimizationRating)
		skipMap[i] = i + 1
	}
	peers := make([]peer.ID, 0, len(optimizedPeers))
	skipMapStart := 0
	//fmt.Printf("%d\n", len(optimizedPeers))
	for range optimizedPeers {
		randValue := rand.Float64()
		randTarget := randValue * optimizationTotal
		//fmt.Printf("opt total %f, rand %f, randTarget %f\n", optimizationTotal, randValue, randTarget)
		//fmt.Printf("skipMapStart %d\n", skipMapStart)
		targetSoFar := 0.0
		currentIndex := skipMapStart
		previousIndex := -1
		for {
			currentRating := transformOptimization(optimizedPeers[currentIndex].OptimizationRating)
			targetSoFar += currentRating
			//fmt.Printf("targetSoFar %f, randTarget %f\n", targetSoFar, randTarget)
			if targetSoFar+0.000001 >= randTarget {
				//fmt.Printf("%d\n", currentIndex)
				peers = append(peers, optimizedPeers[currentIndex].Peer)
				optimizationTotal -= currentRating
				if currentIndex == skipMapStart {
					skipMapStart = skipMap[currentIndex]
				} else {
					skipMap[previousIndex] = skipMap[currentIndex]
				}
				break
			}
			previousIndex = currentIndex
			currentIndex = skipMap[currentIndex]
		}
	}
	return peers
}

type splitRequestMessage struct {
	optimizedPeers []bssd.OptimizedPeer
	ks             []cid.Cid
	resp           chan []bssd.PartialRequest
}

func (s *splitRequestMessage) handle(srs *SessionRequestSplitter) {
	split := srs.split
	// first iteration ignore optimization ratings
	peers := srs.peersFromOptimizedPeers(s.optimizedPeers)
	ks := s.ks

	if len(peers) < split {
		split = len(peers)
	}
	peerSplits := splitPeers(peers, split)
	if len(ks) < split {
		split = len(ks)
	}
	keySplits := splitKeys(ks, split)
	splitRequests := make([]bssd.PartialRequest, 0, len(keySplits))
	for i, keySplit := range keySplits {
		splitRequests = append(splitRequests, bssd.PartialRequest{Peers: peerSplits[i], Keys: keySplit})
	}
	s.resp <- splitRequests
}

type recordDuplicateMessage struct{}

func (r *recordDuplicateMessage) handle(srs *SessionRequestSplitter) {
	srs.receivedCount++
	srs.duplicateReceivedCount++
	if (srs.receivedCount > minReceivedToAdjustSplit) && (srs.duplicateRatio() > maxAcceptableDupes) && (srs.split < maxSplit) {
		srs.split++
	}
}

type recordUniqueMessage struct{}

func (r *recordUniqueMessage) handle(srs *SessionRequestSplitter) {
	srs.receivedCount++
	if (srs.split > 1) && (srs.duplicateRatio() < minDuplesToTryLessSplits) {
		srs.split--
	}
}
func splitKeys(ks []cid.Cid, split int) [][]cid.Cid {
	splits := make([][]cid.Cid, split)
	for i, c := range ks {
		pos := i % split
		splits[pos] = append(splits[pos], c)
	}
	return splits
}

func splitPeers(peers []peer.ID, split int) [][]peer.ID {
	splits := make([][]peer.ID, split)
	for i, p := range peers {
		pos := i % split
		splits[pos] = append(splits[pos], p)
	}
	return splits
}
