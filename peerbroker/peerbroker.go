package wantmanager

import (
	"context"
	"fmt"

	// logging "github.com/ipfs/go-log"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// var log = logging.Logger("bitswap")

type WantSource interface {
	MatchWantPeer([]peer.ID) *Want
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

type Want struct {
	Cid       cid.Cid
	WantHaves []cid.Cid
	Peer      peer.ID
	Ses       uint64
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

const peerBatchSize = 16

func (pb *PeerBroker) checkMatch() {
	batches := make(map[uint64]map[peer.ID][]Want)

	// log.Warningf("           checkMatch (%d sources)\n", len(pb.sources))
	gotWant := true
	for gotWant {
		gotWant = false
		cnt := 0
		for s := range pb.sources {
			peers := pb.wantManager.AvailablePeers()
			// fmt.Printf("      avail peers (%d)\n", len(peers))
			w := s.MatchWantPeer(peers)
			// fmt.Printf("      MatchWantPeer %v\n", w)
			if w != nil {
				gotWant = true
				cnt++

				if _, ok := batches[w.Ses]; !ok {
					batches[w.Ses] = make(map[peer.ID][]Want)
				}
				pw, ok := batches[w.Ses][w.Peer]
				if !ok {
					pw = make([]Want, 0, peerBatchSize)
				}
				pw = append(pw, Want{w.Cid, w.WantHaves, w.Peer, w.Ses})

				if len(pw) == peerBatchSize {
					pb.sendBatch(pb.ctx, w.Ses, pw, w.Peer)
					delete(batches[w.Ses], w.Peer)
				} else {
					batches[w.Ses][w.Peer] = pw
				}
			}
		}
		// log.Warningf("           checkMatch done (gotWant: %t)\n", gotWant)
	}
	// fmt.Printf("  checkMatch [done]\n")
	for s, pw := range batches {
		for p, b := range pw {
			if len(b) > 0 {
				pb.sendBatch(pb.ctx, s, b, p)
			}
		}
	}
}

func (pb *PeerBroker) sendBatch(ctx context.Context, sesid uint64, batch []Want, p peer.ID) {
	cids := make([]cid.Cid, 0, len(batch))
	whaves := make(map[cid.Cid]struct{})
	for _, b := range batch {
		cids = append(cids, b.Cid)
		for _, c := range b.WantHaves {
			whaves[c] = struct{}{}
		}
	}
	wantHaves := make([]cid.Cid, 0)
	for c := range whaves {
		wantHaves = append(wantHaves, c)
	}
	if batch[0].Peer != p {
		panic(fmt.Sprintf("Batch contained different peer than expected: %s / %s", batch[0].Peer, p))
	}
	pb.wantManager.WantBlocks(ctx, cids, wantHaves, true, []peer.ID{batch[0].Peer}, sesid)
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
