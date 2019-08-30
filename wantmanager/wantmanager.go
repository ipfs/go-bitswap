package wantmanager

import (
	"context"
	"math"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bspb "github.com/ipfs/go-bitswap/peerbroker"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"

	cid "github.com/ipfs/go-cid"
	metrics "github.com/ipfs/go-metrics-interface"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")

const (
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = math.MaxInt32
)

// PeerHandler sends changes out to the network as they get added to the wantlist
// managed by the WantManager.
type PeerHandler interface {
	Disconnected(p peer.ID)
	Connected(p peer.ID, initialWants *wantlist.SessionTrackedWantlist)
	SendMessage(entries []bsmsg.Entry, targets []peer.ID, sessid uint64)
}

type wantMessage interface {
	handle(wm *WantManager)
}

// WantManager manages a global want list. It tracks two seperate want lists -
// one for all wants, and one for wants that are specifically broadcast to the
// internet.
type WantManager struct {
	// channel requests to the run loop
	// to get predictable behavior while running this in a go routine
	// having only one channel is neccesary, so requests are processed serially
	wantMessages chan wantMessage

	// synchronized by Run loop, only touch inside there
	wl   *wantlist.SessionTrackedWantlist
	bcwl *wantlist.SessionTrackedWantlist

	pal        bspb.PeerAvailabilityListener
	peerCounts map[peer.ID]int

	ctx    context.Context
	cancel func()

	peerHandler   PeerHandler
	wantlistGauge metrics.Gauge
}

// New initializes a new WantManager for a given context.
func New(ctx context.Context, peerHandler PeerHandler) *WantManager {
	ctx, cancel := context.WithCancel(ctx)
	wantlistGauge := metrics.NewCtx(ctx, "wantlist_total",
		"Number of items in wantlist.").Gauge()
	return &WantManager{
		wantMessages:  make(chan wantMessage, 10),
		wl:            wantlist.NewSessionTrackedWantlist(),
		bcwl:          wantlist.NewSessionTrackedWantlist(),
		ctx:           ctx,
		cancel:        cancel,
		peerHandler:   peerHandler,
		wantlistGauge: wantlistGauge,
		peerCounts:    make(map[peer.ID]int),
	}
}

// WantBlocks adds the given cids to the wantlist, tracked by the given session.
func (wm *WantManager) WantBlocks(ctx context.Context, ks []cid.Cid, wantHaves []cid.Cid, sendDontHave bool, peers []peer.ID, ses uint64) {
	log.Debugf("[wantlist] want blocks; cids=%s, peers=%s, ses=%d", ks, peers, ses)
	wm.addEntries(ctx, ks, wantHaves, sendDontHave, peers, false, ses)
}

// CancelWants removes the given cids from the wantlist, tracked by the given session.
func (wm *WantManager) CancelWants(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64) {
	log.Debugf("[wantlist] unwant blocks; cids=%s, peers=%s, ses=%d", ks, peers, ses)
	wm.addEntries(context.Background(), ks, []cid.Cid{}, false, peers, true, ses)
}

// CancelWantHaves removes the given want-have cids from the wantlist, tracked by the given session.
func (wm *WantManager) CancelWantHaves(ctx context.Context, wantHaves []cid.Cid, peers []peer.ID, ses uint64) {
	log.Debugf("[wantlist] rm want-haves; want-haves=%s, peers=%s, ses=%d", wantHaves, peers, ses)
	wm.addEntries(context.Background(), nil, wantHaves, false, peers, true, ses)
}

// CurrentWants returns the list of current wants.
func (wm *WantManager) CurrentWants() []wantlist.Entry {
	resp := make(chan []wantlist.Entry, 1)
	select {
	case wm.wantMessages <- &currentWantsMessage{resp}:
	case <-wm.ctx.Done():
		return nil
	}
	select {
	case wantlist := <-resp:
		return wantlist
	case <-wm.ctx.Done():
		return nil
	}
}

// CurrentBroadcastWants returns the current list of wants that are broadcasts.
func (wm *WantManager) CurrentBroadcastWants() []wantlist.Entry {
	resp := make(chan []wantlist.Entry, 1)
	select {
	case wm.wantMessages <- &currentBroadcastWantsMessage{resp}:
	case <-wm.ctx.Done():
		return nil
	}
	select {
	case wl := <-resp:
		return wl
	case <-wm.ctx.Done():
		return nil
	}
}

func (wm *WantManager) AvailablePeers() []peer.ID {
	resp := make(chan []peer.ID, 1)
	select {
	case wm.wantMessages <- &availablePeersMessage{resp}:
	case <-wm.ctx.Done():
		return []peer.ID{}
	}
	select {
	case ps := <-resp:
		return ps
	case <-wm.ctx.Done():
		return []peer.ID{}
	}
}

func (wm *WantManager) RegisterPeerAvailabilityListener(l bspb.PeerAvailabilityListener) {
	select {
	case wm.wantMessages <- &registerPAL{l}:
	case <-wm.ctx.Done():
	}
}

// Connected is called when a new peer is connected
func (wm *WantManager) Connected(p peer.ID) {
	select {
	case wm.wantMessages <- &connectedMessage{p}:
	case <-wm.ctx.Done():
	}
}

// Disconnected is called when a peer is disconnected
func (wm *WantManager) Disconnected(p peer.ID) {
	select {
	case wm.wantMessages <- &disconnectedMessage{p}:
	case <-wm.ctx.Done():
	}
}

// Startup starts processing for the WantManager.
func (wm *WantManager) Startup() {
	go wm.run()
}

// Shutdown ends processing for the want manager.
func (wm *WantManager) Shutdown() {
	wm.cancel()
}

func (wm *WantManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	for {
		select {
		case message := <-wm.wantMessages:
			message.handle(wm)
		case <-wm.ctx.Done():
			return
		}
	}
}

func (wm *WantManager) addEntries(ctx context.Context, ks []cid.Cid, wantHaves []cid.Cid, sendDontHave bool, targets []peer.ID, cancel bool, ses uint64) {
	// TODO: Keep track of which wantHaves have been sent to which peers

	entries := make([]bsmsg.Entry, 0, len(ks))
	for i, k := range ks {
		entries = append(entries, bsmsg.Entry{
			Cancel: cancel,
			Entry:  wantlist.NewRefEntry(k, maxPriority-i, false, sendDontHave),
		})
	}
	for i, k := range wantHaves {
		entries = append(entries, bsmsg.Entry{
			Cancel: cancel,
			Entry:  wantlist.NewRefEntry(k, maxPriority-i, true, sendDontHave),
		})
	}
	select {
	case wm.wantMessages <- &wantSet{entries: entries, targets: targets, sessid: ses}:
	case <-wm.ctx.Done():
	case <-ctx.Done():
	}
}

type wantSet struct {
	entries []bsmsg.Entry
	targets []peer.ID
	sessid  uint64
}

func (ws *wantSet) handle(wm *WantManager) {
	// is this a broadcast or not?
	brdc := len(ws.targets) == 0

	// add changes to our wantlist
	for _, e := range ws.entries {
		if e.Cancel {
			if e.WantType == wantlist.WantType_Block {
				// We only ever broadcast want-haves. So don't remove broadcast
				// wantlist entries until we receive a cancel for the block itself.
				wm.bcwl.Remove(e.Cid, ws.sessid, false)

				// For the global want-list we disregard want-haves
				if wm.wl.Remove(e.Cid, ws.sessid, false) {
					wm.wantlistGauge.Dec()
				}
			}
		} else {
			if brdc {
				wm.bcwl.AddEntry(e.Entry, ws.sessid)
			}
			// For the global want-list we disregard want-haves
			if e.WantType == wantlist.WantType_Block {
				if wm.wl.AddEntry(e.Entry, ws.sessid) {
					wm.wantlistGauge.Inc()
				}
			}
		}
	}

	// Very crudely implemented rate-limiting. Doesn't ever decrement.
	// TODO: get this from the want list instead of maintaining
	// it separately here
	// TODO: handle disconnects
	// TODO: handle dups
	// for _, e := range ws.entries {
	// 	if e.WantType == wantlist.WantType_Block {
	// 		for _, p := range ws.targets {
	// 			if _, ok := wm.peerCounts[p]; !ok {
	// 				wm.peerCounts[p] = 0
	// 			}
	// 			if e.Cancel {
	// 				// TODO: handle cancel-block (it's a broadcast to all peers so won't get decremented here)
	// 				wm.peerCounts[p]--
	// 			} else {
	// 				wm.peerCounts[p]++
	// 			}
	// 		}
	// 	}
	// }

	// for p, c := range wm.peerCounts {
	// 	log.Warningf("want-manager: %s live = %d", p, c)
	// }

	// broadcast those wantlist changes
	wm.peerHandler.SendMessage(ws.entries, ws.targets, ws.sessid)
}

type availablePeersMessage struct {
	resp chan<- []peer.ID
}

const maxLiveWantsPerPeer = 1024

func (apm *availablePeersMessage) handle(wm *WantManager) {
	// Very simple rate-limit on peers
	// TODO: get this from the want list instead of maintaining
	// it separately here
	peers := make([]peer.ID, 0, len(wm.peerCounts))
	for p, c := range wm.peerCounts {
		if c < maxLiveWantsPerPeer {
			peers = append(peers, p)
		}
	}
	apm.resp <- peers
}

type currentWantsMessage struct {
	resp chan<- []wantlist.Entry
}

func (cwm *currentWantsMessage) handle(wm *WantManager) {
	cwm.resp <- wm.wl.Entries()
}

type currentBroadcastWantsMessage struct {
	resp chan<- []wantlist.Entry
}

func (cbcwm *currentBroadcastWantsMessage) handle(wm *WantManager) {
	cbcwm.resp <- wm.bcwl.Entries()
}

type registerPAL struct {
	l bspb.PeerAvailabilityListener
}

func (cm *registerPAL) handle(wm *WantManager) {
	wm.pal = cm.l
}

type connectedMessage struct {
	p peer.ID
}

func (cm *connectedMessage) handle(wm *WantManager) {
	wm.peerHandler.Connected(cm.p, wm.bcwl)

	if _, ok := wm.peerCounts[cm.p]; !ok {
		wm.peerCounts[cm.p] = 0
	}
	if wm.pal != nil {
		wm.pal.PeerAvailable()
	}
}

type disconnectedMessage struct {
	p peer.ID
}

func (dm *disconnectedMessage) handle(wm *WantManager) {
	wm.peerHandler.Disconnected(dm.p)
}
