package peerbroker

import (
	"bytes"
	"context"
	"fmt"
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
	WantAvailable()
}

type WantManager interface {
	WantBlocks(context.Context, peer.ID, uint64, []cid.Cid, []cid.Cid)
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

func (a *SessionAsk) String() string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("->%s: want-block %s\n", a.Peer, a.Cid))
	for _, c := range a.WantHaves {
		b.WriteString(fmt.Sprintf("  want-have %s\n", c))
	}
	for _, p := range a.PeerHaves {
		b.WriteString(fmt.Sprintf("  peer-have %s\n", p))
	}
	return b.String()
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
	// TODO: only ask given sessions
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

func (pb *PeerBroker) checkMatch() {
	gotWant := true
	for gotWant {
		gotWant = false
		cnt := 0
		for s := range pb.sources {
			peers := pb.wantManager.AvailablePeers()
			ask := s.MatchWantPeer(peers)
			if ask != nil {
				gotWant = true
				cnt++

				pb.wantManager.WantBlocks(pb.ctx, ask.Peer, s.ID(), []cid.Cid{ask.Cid}, ask.WantHaves)

				for _, p := range ask.PeerHaves {
					pb.wantManager.WantBlocks(pb.ctx, p, s.ID(), []cid.Cid{}, []cid.Cid{ask.Cid})
				}
			}
		}
	}
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
