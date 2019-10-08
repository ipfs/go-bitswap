package wantmanager

import (
	"context"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	bssim "github.com/ipfs/go-bitswap/sessioninterestmanager"
	bssm "github.com/ipfs/go-bitswap/sessionmanager"
	bsswl "github.com/ipfs/go-bitswap/sessionwantlist"
	logging "github.com/ipfs/go-log"

	cid "github.com/ipfs/go-cid"
	metrics "github.com/ipfs/go-metrics-interface"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")

// PeerHandler sends changes out to the network as they get added to the wantlist
// managed by the WantManager.
type PeerHandler interface {
	Disconnected(p peer.ID)
	Connected(p peer.ID, initialWants []cid.Cid)
	BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid)
	SendWants(ctx context.Context, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid)
	SendCancels(context.Context, []cid.Cid)
	Trace()
}

type wantMessage interface {
	handle(wm *WantManager)
}

// WantManager manages a global want list. It tracks two seperate want lists -
// one for all wants, and one for wants that are specifically broadcast to the
// internet.
type WantManager struct {
	bcwl *bsswl.SessionWantlist

	peerHandler PeerHandler
	sim         *bssim.SessionInterestManager
	bpm         *bsbpm.BlockPresenceManager
	sm          *bssm.SessionManager
	// TODO: update wantlistGauge
	wantlistGauge metrics.Gauge
}

// New initializes a new WantManager for a given context.
func New(ctx context.Context, peerHandler PeerHandler, sim *bssim.SessionInterestManager, bpm *bsbpm.BlockPresenceManager) *WantManager {
	wantlistGauge := metrics.NewCtx(ctx, "wantlist_total",
		"Number of items in wantlist.").Gauge()
	return &WantManager{
		bcwl:          bsswl.NewSessionWantlist(),
		peerHandler:   peerHandler,
		sim:           sim,
		bpm:           bpm,
		wantlistGauge: wantlistGauge,
	}
}

func (wm *WantManager) SetSessionManager(sm *bssm.SessionManager) {
	wm.sm = sm
}

func (wm *WantManager) ReceiveFrom(ctx context.Context, p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	// Record block presence for HAVE / DONT_HAVE
	wm.bpm.ReceiveFrom(p, haves, dontHaves)
	// Inform interested sessions
	// sessions := wm.sm.ReceiveFrom(p, blks, haves, dontHaves)
	_ = wm.sm.ReceiveFrom(p, blks, haves, dontHaves)
	// Remove received blocks from broadcast wantlist
	wm.bcwl.RemoveKeys(blks)
	// Send CANCEL to all peers with want-have / want-block
	wm.peerHandler.SendCancels(ctx, blks)
}

func (wm *WantManager) BroadcastWantHaves(ctx context.Context, ses uint64, wantHaves []cid.Cid) {
	// log.Warningf("BroadcastWantHaves session%d: %s", ses, wantHaves)

	// Record broadcast wants
	wm.bcwl.Add(wantHaves, ses)

	// Send want-haves to all peers
	wm.peerHandler.BroadcastWantHaves(ctx, wantHaves)
}

func (wm *WantManager) RemoveSession(ctx context.Context, ses uint64) {
	// Remove session's interest in the given blocks
	cancelKs := wm.sim.RemoveSessionInterest(ses)

	// Remove broadcast want-haves for session
	wm.bcwl.RemoveSession(ses)

	// Free up block presence tracking for keys that no session is interested
	// in anymore
	// TODO: Do these asynchronously:
	// - wm.bpm.RemoveKeys(cancelKs)
	// - wm.peerHandler.SendCancels(ctx, cancelKs)
	// to cover the case where one thread removes keys while another is asking for those same keys
	wm.bpm.RemoveKeys(cancelKs)

	// Send CANCEL to all peers for blocks that no session is interested in anymore
	// TODO: may not be worth sending cancels: test performance
	wm.peerHandler.SendCancels(ctx, cancelKs)
}

func (wm *WantManager) Trace() {
	wm.peerHandler.Trace()
}

// Connected is called when a new peer is connected
func (wm *WantManager) Connected(p peer.ID) {
	// Tell the peer handler that there is a new connection and give it the
	// list of outstanding broadcast wants
	wm.peerHandler.Connected(p, wm.bcwl.Keys())
}

// Disconnected is called when a peer is disconnected
func (wm *WantManager) Disconnected(p peer.ID) {
	wm.peerHandler.Disconnected(p)
}
