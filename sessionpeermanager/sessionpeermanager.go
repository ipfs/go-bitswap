package sessionpeermanager

import (
	"context"
	"fmt"
	"math/rand"

	cid "github.com/ipfs/go-cid"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	maxOptimizedPeers        = 32
	reservePeers             = 2
	minReceivedToAdjustSplit = 2
	maxSplit                 = 16
	maxAcceptableDupes       = 0.4
	minDuplesToTryLessSplits = 0.2
	initialSplit             = 2
)

// PartialRequest is represents one slice of an over request split among peers
type PartialRequest struct {
	Peers []peer.ID
	Keys  []cid.Cid
}

// PeerNetwork is an interface for finding providers and managing connections
type PeerNetwork interface {
	ConnectionManager() ifconnmgr.ConnManager
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID
}

type peerMessage interface {
	handle(spm *SessionPeerManager)
}

// SessionPeerManager tracks and manages peers for a session, and provides
// the best ones to the session
type SessionPeerManager struct {
	ctx     context.Context
	network PeerNetwork
	tag     string

	peerMessages chan peerMessage

	// do not touch outside of run loop
	activePeers            map[peer.ID]bool
	unoptimizedPeersArr    []peer.ID
	optimizedPeersArr      []peer.ID
	receivedCount          int
	split                  int
	duplicateReceivedCount int
}

// New creates a new SessionPeerManager
func New(ctx context.Context, id uint64, network PeerNetwork) *SessionPeerManager {
	spm := &SessionPeerManager{
		ctx:          ctx,
		network:      network,
		peerMessages: make(chan peerMessage, 16),
		activePeers:  make(map[peer.ID]bool),
		split:        initialSplit,
	}

	spm.tag = fmt.Sprint("bs-ses-", id)

	go spm.run(ctx)
	return spm
}

// RecordPeerResponse records that a peer received a block, and adds to it
// the list of peers if it wasn't already added
func (spm *SessionPeerManager) RecordPeerResponse(p peer.ID, k cid.Cid) {

	// at the moment, we're just adding peers here
	// in the future, we'll actually use this to record metrics
	select {
	case spm.peerMessages <- &peerResponseMessage{p}:
	case <-spm.ctx.Done():
	}
}

// RecordPeerRequests records that a given set of peers requested the given cids
func (spm *SessionPeerManager) RecordPeerRequests(p []peer.ID, ks []cid.Cid) {
	// at the moment, we're not doing anything here
	// soon we'll use this to track latency by peer
}

// HasPeers returns whether there are peers present for this session
func (spm *SessionPeerManager) HasPeers() bool {
	// right now this just returns all peers, but soon we might return peers
	// ordered by optimization, or only a subset
	resp := make(chan bool)
	select {
	case spm.peerMessages <- &hasPeersMessage{resp}:
	case <-spm.ctx.Done():
		return false
	}

	select {
	case hasPeers := <-resp:
		return hasPeers
	case <-spm.ctx.Done():
		return false
	}
}

// SplitRequestAmongPeers splits a request for the given cids among the peers
func (spm *SessionPeerManager) SplitRequestAmongPeers(ks []cid.Cid) []*PartialRequest {
	resp := make(chan []*PartialRequest)

	select {
	case spm.peerMessages <- &splitRequestMessage{ks, resp}:
	case <-spm.ctx.Done():
		return nil
	}
	select {
	case splitRequests := <-resp:
		return splitRequests
	case <-spm.ctx.Done():
		return nil
	}
}

// RecordDuplicateBlock records the fact that the session received a duplicate
// block and adjusts split factor as neccesary.
func (spm *SessionPeerManager) RecordDuplicateBlock() {
	select {
	case spm.peerMessages <- &recordDuplicateMessage{}:
	case <-spm.ctx.Done():
	}
}

// FindMorePeers attempts to find more peers for a session by searching for
// providers for the given Cid
func (spm *SessionPeerManager) FindMorePeers(ctx context.Context, c cid.Cid) {
	go func(k cid.Cid) {
		// TODO: have a task queue setup for this to:
		// - rate limit
		// - manage timeouts
		// - ensure two 'findprovs' calls for the same block don't run concurrently
		// - share peers between sessions based on interest set
		for p := range spm.network.FindProvidersAsync(ctx, k, 10) {
			spm.peerMessages <- &peerFoundMessage{p}
		}
	}(c)
}

func (spm *SessionPeerManager) run(ctx context.Context) {
	for {
		select {
		case pm := <-spm.peerMessages:
			pm.handle(spm)
		case <-ctx.Done():
			spm.handleShutdown()
			return
		}
	}
}

func (spm *SessionPeerManager) tagPeer(p peer.ID) {
	cmgr := spm.network.ConnectionManager()
	cmgr.TagPeer(p, spm.tag, 10)
}

func (spm *SessionPeerManager) insertOptimizedPeer(p peer.ID) {
	if len(spm.optimizedPeersArr) >= (maxOptimizedPeers - reservePeers) {
		tailPeer := spm.optimizedPeersArr[len(spm.optimizedPeersArr)-1]
		spm.optimizedPeersArr = spm.optimizedPeersArr[:len(spm.optimizedPeersArr)-1]
		spm.unoptimizedPeersArr = append(spm.unoptimizedPeersArr, tailPeer)
	}

	spm.optimizedPeersArr = append([]peer.ID{p}, spm.optimizedPeersArr...)
}

func (spm *SessionPeerManager) removeOptimizedPeer(p peer.ID) {
	for i := 0; i < len(spm.optimizedPeersArr); i++ {
		if spm.optimizedPeersArr[i] == p {
			spm.optimizedPeersArr = append(spm.optimizedPeersArr[:i], spm.optimizedPeersArr[i+1:]...)
			return
		}
	}
}

func (spm *SessionPeerManager) removeUnoptimizedPeer(p peer.ID) {
	for i := 0; i < len(spm.unoptimizedPeersArr); i++ {
		if spm.unoptimizedPeersArr[i] == p {
			spm.unoptimizedPeersArr[i] = spm.unoptimizedPeersArr[len(spm.unoptimizedPeersArr)-1]
			spm.unoptimizedPeersArr = spm.unoptimizedPeersArr[:len(spm.unoptimizedPeersArr)-1]
			return
		}
	}
}

func (spm *SessionPeerManager) duplicateBlockReceived() {
	spm.receivedCount++
	spm.duplicateReceivedCount++
	if (spm.receivedCount > minReceivedToAdjustSplit) && (spm.duplicateRatio() > maxAcceptableDupes) && (spm.split < maxSplit) {
		spm.split++
	}
}

func (spm *SessionPeerManager) uniqueBlockReceived() {
	spm.receivedCount++
	if (spm.split > 1) && (spm.duplicateRatio() < minDuplesToTryLessSplits) {
		spm.split--
	}
}

func (spm *SessionPeerManager) duplicateRatio() float64 {
	return float64(spm.duplicateReceivedCount) / float64(spm.receivedCount)
}

func (spm *SessionPeerManager) peerCount() int {
	return len(spm.unoptimizedPeersArr) + len(spm.optimizedPeersArr)
}

func (spm *SessionPeerManager) targetSplit() int {
	if spm.split <= spm.peerCount() {
		return spm.split
	}
	return spm.peerCount()
}

func (spm *SessionPeerManager) getExtraPeers(peerCount int) []peer.ID {
	randomOrder := rand.Perm(len(spm.unoptimizedPeersArr))
	if peerCount > len(spm.unoptimizedPeersArr) {
		peerCount = len(spm.unoptimizedPeersArr)
	}
	extraPeers := make([]peer.ID, peerCount)
	for i := range extraPeers {
		extraPeers[i] = spm.unoptimizedPeersArr[randomOrder[i]]
	}
	return extraPeers
}

type peerFoundMessage struct {
	p peer.ID
}

func (pfm *peerFoundMessage) handle(spm *SessionPeerManager) {
	p := pfm.p
	if _, ok := spm.activePeers[p]; !ok {
		spm.activePeers[p] = false
		spm.unoptimizedPeersArr = append(spm.unoptimizedPeersArr, p)
		spm.tagPeer(p)
	}
}

type peerResponseMessage struct {
	p peer.ID
}

func (prm *peerResponseMessage) handle(spm *SessionPeerManager) {
	spm.uniqueBlockReceived()
	p := prm.p
	isOptimized, ok := spm.activePeers[p]
	if !ok {
		spm.activePeers[p] = true
		spm.tagPeer(p)
	} else {
		if isOptimized {
			spm.removeOptimizedPeer(p)
		} else {
			spm.activePeers[p] = true
			spm.removeUnoptimizedPeer(p)
		}
	}
	spm.insertOptimizedPeer(p)
}

type splitRequestMessage struct {
	ks   []cid.Cid
	resp chan []*PartialRequest
}

func (s *splitRequestMessage) handle(spm *SessionPeerManager) {
	ks := s.ks
	if spm.peerCount() == 0 {
		s.resp <- []*PartialRequest{
			&PartialRequest{
				Peers: nil,
				Keys:  ks,
			},
		}
		return
	}
	split := spm.targetSplit()
	extraPeers := spm.getExtraPeers(maxOptimizedPeers - len(spm.optimizedPeersArr))
	allPeers := append(spm.optimizedPeersArr, extraPeers...)
	peerSplits := splitPeers(allPeers, split)
	if len(ks) < split {
		split = len(ks)
	}
	keySplits := splitKeys(ks, split)
	splitRequests := make([]*PartialRequest, len(keySplits))
	for i := range splitRequests {
		splitRequests[i] = &PartialRequest{peerSplits[i], keySplits[i]}
	}
	s.resp <- splitRequests
}

type hasPeersMessage struct {
	resp chan<- bool
}

func (hpm *hasPeersMessage) handle(spm *SessionPeerManager) {
	hpm.resp <- spm.peerCount() > 0
}

type recordDuplicateMessage struct{}

func (rdm *recordDuplicateMessage) handle(spm *SessionPeerManager) {
	spm.duplicateBlockReceived()
}

func (spm *SessionPeerManager) handleShutdown() {
	cmgr := spm.network.ConnectionManager()
	for p := range spm.activePeers {
		cmgr.UntagPeer(p, spm.tag)
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
