package wantmanager

import (
	"context"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	bssim "github.com/ipfs/go-bitswap/sessioninterestmanager"
	"github.com/ipfs/go-bitswap/sessionmanager"
	bsswl "github.com/ipfs/go-bitswap/sessionwantlist"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type PeerHandler interface {
	Connected(p peer.ID, initialWants []cid.Cid)
	Disconnected(p peer.ID)
	BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid)
	SendCancels(context.Context, []cid.Cid)
}

type SessionManager interface {
	ReceiveFrom(p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []sessionmanager.Session
}

// WantManager
// - informs the SessionManager and BlockPresenceManager of incoming information
//   and cancelled sessions
// - informs the PeerManager of connects and disconnects
// - manages the list of want-haves that are broadcast to the internet
//   (as opposed to being sent to specific peers)
type WantManager struct {
	bcwl *bsswl.SessionWantlist

	peerHandler PeerHandler
	sim         *bssim.SessionInterestManager
	bpm         *bsbpm.BlockPresenceManager
	sm          SessionManager
}

// New initializes a new WantManager for a given context.
func New(ctx context.Context, peerHandler PeerHandler, sim *bssim.SessionInterestManager, bpm *bsbpm.BlockPresenceManager) *WantManager {
	return &WantManager{
		bcwl:        bsswl.NewSessionWantlist(),
		peerHandler: peerHandler,
		sim:         sim,
		bpm:         bpm,
	}
}

func (wm *WantManager) SetSessionManager(sm SessionManager) {
	wm.sm = sm
}

func (wm *WantManager) ReceiveFrom(ctx context.Context, p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	// Record block presence for HAVE / DONT_HAVE
	wm.bpm.ReceiveFrom(p, haves, dontHaves)
	// Inform interested sessions
	wm.sm.ReceiveFrom(p, blks, haves, dontHaves)
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
	wm.bpm.RemoveKeys(cancelKs)

	// Send CANCEL to all peers for blocks that no session is interested in anymore
	wm.peerHandler.SendCancels(ctx, cancelKs)
}

// Connected is called when a new peer connects
func (wm *WantManager) Connected(p peer.ID) {
	// Tell the peer handler that there is a new connection and give it the
	// list of outstanding broadcast wants
	wm.peerHandler.Connected(p, wm.bcwl.Keys())
}

// Disconnected is called when a peer disconnects
func (wm *WantManager) Disconnected(p peer.ID) {
	wm.peerHandler.Disconnected(p)
}
