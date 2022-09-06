package reputationmanager

import (
	"context"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

type ReputationManager struct {
	rwl         *recentWantList
	scoreKeeper ScoreKeeper
	params      *ReputationManagerParams
	thresholds  *ReputationManagerThresholds
}

type ReputationManagerParams struct {
	RetainScore          time.Duration
	ScoreRefreshInterval time.Duration
}

type ReputationManagerThresholds struct {
	GrayListThreshold float64
}

type ScoreKeeper interface {
	Score(p peer.ID) float64
	Update(p peer.ID, wantedBlks, unwantedBlks []blocks.Block, wantedPresences, unwantedPresences []cid.Cid)
	PeerConnected(p peer.ID)
	PeerDisconnected(p peer.ID)
}

func NewReputationManager(params *ReputationManagerParams, thresholds *ReputationManagerThresholds, scoreKeeper ScoreKeeper) *ReputationManager {
	rm := &ReputationManager{
		params:      params,
		thresholds:  thresholds,
		scoreKeeper: scoreKeeper,
		// TODO: make these params configurable
		rwl: newRecentWantList(&recentWantListParams{
			numEntries:      float64(1 << 12),
			wrongPositives:  float64(0.01),
			refreshInterval: 2 * time.Minute,
		}),
	}

	return rm
}

func (r *ReputationManager) AcceptFrom(pid peer.ID) bool {
	return r.scoreKeeper.Score(pid) > r.thresholds.GrayListThreshold
}

func (r *ReputationManager) AddWants(cids []cid.Cid) {
	r.rwl.addWants(cids)
}

func (r *ReputationManager) ReceivedFrom(pid peer.ID, blks []blocks.Block, presences []cid.Cid) {
	wantedBlks, unwantedBlks, wantedPresences, unwantedPresences := r.rwl.splitRecentlyWanted(blks, presences)
	r.scoreKeeper.Update(pid, wantedBlks, unwantedBlks, wantedPresences, unwantedPresences)
}

func (r *ReputationManager) PeerConnected(pid peer.ID) {
	r.scoreKeeper.PeerConnected(pid)
}

func (r *ReputationManager) PeerDisconnected(pid peer.ID) {
	r.scoreKeeper.PeerDisconnected(pid)
}

func (r *ReputationManager) Start(ctx context.Context) {
	if r == nil {
		return
	}

	go r.rwl.loop(ctx)
}
