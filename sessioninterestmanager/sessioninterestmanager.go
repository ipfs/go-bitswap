package sessioninterestmanager

import (
	bsswl "github.com/ipfs/go-bitswap/sessionwantlist"

	cid "github.com/ipfs/go-cid"
)

type SessionInterestManager struct {
	wants *bsswl.SessionWantlist
}

// New initializes a new SessionInterestManager.
func New() *SessionInterestManager {
	return &SessionInterestManager{
		wants: bsswl.NewSessionWantlist(),
	}
}

func (sim *SessionInterestManager) RecordSessionInterest(ses uint64, ks []cid.Cid) {
	sim.wants.Add(ks, ses)
}

func (sim *SessionInterestManager) RemoveSessionInterest(ses uint64) []cid.Cid {
	return sim.wants.RemoveSession(ses)
}

func (sim *SessionInterestManager) InterestedSessions(blocks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []uint64 {
	ks := make([]cid.Cid, 0, len(blocks)+len(haves)+len(dontHaves))
	for _, c := range blocks {
		ks = append(ks, c)
	}
	for _, c := range haves {
		ks = append(ks, c)
	}
	for _, c := range dontHaves {
		ks = append(ks, c)
	}

	return sim.wants.SessionsFor(ks)
}
