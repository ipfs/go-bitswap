package wantmanager

import (
	"context"
	// "fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bs:pbkr")

const maxPeerBatchSize = 64
const peerBatchDebounce = 5 * time.Millisecond

type WantSource interface {
	MatchWantPeer([]peer.ID) *SessionAsk
	ID() uint64
}

type PeerAvailabilityListener interface {
	PeerAvailable()
}

type WantManager interface {
	WantBlocks(context.Context, []cid.Cid, []cid.Cid, bool, []peer.ID, uint64)
	AvailablePeers() []peer.ID
	RegisterPeerAvailabilityListener(PeerAvailabilityListener)
}

type message interface {
	handle(*PeerBroker)
}

type SessionAsk struct {
	Cid       cid.Cid
	Peer      peer.ID
	WantHaves []cid.Cid
	PeerHaves []peer.ID
}

type PeerBroker struct {
	// channel requests to the run loop
	// to get predictable behavior while running this in a go routine
	// having only one channel is neccesary, so requests are processed serially
	messages   chan message
	matchReady chan struct{}

	ctx    context.Context
	cancel func()

	wantManager WantManager

	// Don't access/modify outside of run loop
	sources map[WantSource]struct{}
	btchr *batcher
}

// New initializes a new PeerBroker for a given context.
func New(ctx context.Context, wantManager WantManager) *PeerBroker {
	ctx, cancel := context.WithCancel(ctx)
	return &PeerBroker{
		messages:    make(chan message, 16),
		matchReady:  make(chan struct{}, 1),
		ctx:         ctx,
		cancel:      cancel,
		wantManager: wantManager,
		sources:     make(map[WantSource]struct{}),
		btchr:       newBatcher(ctx, wantManager),
	}
}

func (pb *PeerBroker) RegisterSource(s WantSource) {
	select {
	case pb.messages <- &registerSourceMessage{s}:
	case <-pb.ctx.Done():
	}
}

func (pb *PeerBroker) UnregisterSource(s WantSource) {
	select {
	case pb.messages <- &unregisterSourceMessage{s}:
	case <-pb.ctx.Done():
	}
}

func (pb *PeerBroker) PeerAvailable() {
	// fmt.Println("PeerAvailable")
	pb.signalMatchReady()
}

func (pb *PeerBroker) WantAvailable() {
	// fmt.Println("WantAvailable")
	pb.signalMatchReady()
}

func (pb *PeerBroker) signalMatchReady() {
	select {
	case pb.matchReady <- struct{}{}:
	default:
	}
}

// Startup starts processing for the PeerBroker.
func (pb *PeerBroker) Startup() {
	pb.wantManager.RegisterPeerAvailabilityListener(pb)
	go pb.run()
}

// Shutdown ends processing for the PeerBroker.
func (pb *PeerBroker) Shutdown() {
	pb.btchr.shutdown()
	pb.cancel()

	for s := range pb.sources {
		delete(pb.sources, s)
	}
}

func (pb *PeerBroker) run() {
	for {
		select {
		case message := <-pb.messages:
			message.handle(pb)
		case <-pb.matchReady:
			pb.checkMatch()
		case <-pb.ctx.Done():
			return
		}
	}
}

func (pb *PeerBroker) checkMatch() {
	// log.Warningf("           checkMatch (%d sources)\n", len(pb.sources))
	gotWant := true
	for gotWant {
		gotWant = false
		cnt := 0
		for s := range pb.sources {
			peers := pb.wantManager.AvailablePeers()
			// fmt.Printf("      avail peers (%d)\n", len(peers))
			ask := s.MatchWantPeer(peers)
			// fmt.Printf("      MatchWantPeer %v\n", w)
			if ask != nil {
				gotWant = true
				cnt++
				// pb.addRequest(batches, s.ID(), ask.Peer, batchWant{ask.Cid, ask.WantHaves})
				pb.btchr.addRequest(ask.Peer, batchWant{ask.Cid, ask.WantHaves, s.ID()})
				// log.Warningf("      want-blk->%s want-haves->%s: %s\n", ask.Peer, ask.PeerHaves, ask.Cid.String()[2:8])
				for _, p := range ask.PeerHaves {
// log.Warningf("      ask.PeerHaves %s %s\n", p, ask.Cid.String()[2:8])
					// pb.addRequest(batches, s.ID(), p, batchWant{cid.Cid{}, []cid.Cid{ask.Cid}})
					pb.btchr.addRequest(p, batchWant{cid.Cid{}, []cid.Cid{ask.Cid}, s.ID()})
				}
			}
		}
		// log.Warningf("           checkMatch done (gotWant: %t)\n", gotWant)
	}
	// fmt.Printf("  checkMatch [done]\n")
	// for s, pw := range batches {
	// 	for p, b := range pw {
	// 		if len(b) > 0 {
	// 			pb.sendBatch(pb.ctx, s, p, b)
	// 		}
	// 	}
	// }
}

type batcher struct {
	sync.Mutex
	ctx context.Context
	wantManager WantManager
	batches map[peer.ID]*batchInfo
}

type batchInfo struct {
	timer *time.Timer
	wants []batchWant
}

type batchWant struct {
	Cid       cid.Cid
	WantHaves []cid.Cid
	SesId     uint64
}

func newBatcher(ctx context.Context, wm WantManager) *batcher {
	return &batcher{
		ctx: ctx,
		batches: make(map[peer.ID]*batchInfo),
		wantManager: wm,
	}
}

func (b *batcher) shutdown() {
	for p := range b.batches {
		b.batches[p].timer.Stop()
		delete(b.batches, p)
	}
}

func (b *batcher) addRequest(p peer.ID, w batchWant) {
	b.Lock()
	defer b.Unlock()

	bi, ok := b.batches[p]
	if !ok {
		bi = &batchInfo{
			wants: make([]batchWant, 1),
			timer: time.AfterFunc(peerBatchDebounce, func() {
				b.sendBatch(p)
			}),
		}
		b.batches[p] = bi
	}
	bi.wants = append(bi.wants, w)

	if len(bi.wants) == maxPeerBatchSize {
		b.sendBatch(p)
	}
}

func (b *batcher) sendBatch(p peer.ID) {
	b.Lock()
	defer b.Unlock()

	b.batches[p].timer.Stop()

	cids := cid.NewSet()
	wantHaves := cid.NewSet()
	batchWants := b.batches[p].wants
	for _, b := range batchWants {
		if b.Cid.Defined() {
			cids.Add(b.Cid)
		}
	}
	for _, b := range batchWants {
		for _, c := range b.WantHaves {
			if !cids.Has(c) {
				wantHaves.Add(c)
			}
		}
	}

	// TODO: Respect session id
	sesid := uint64(1)
	b.wantManager.WantBlocks(b.ctx, cids.Keys(), wantHaves.Keys(), true, []peer.ID{p}, sesid)

	delete(b.batches, p)
}

type registerSourceMessage struct {
	source WantSource
}

func (m *registerSourceMessage) handle(pb *PeerBroker) {
	pb.sources[m.source] = struct{}{}
}

type unregisterSourceMessage struct {
	source WantSource
}

func (m *unregisterSourceMessage) handle(pb *PeerBroker) {
	delete(pb.sources, m.source)
}
