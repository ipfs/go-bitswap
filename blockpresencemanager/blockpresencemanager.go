package blockpresencemanager

import (
	"sync"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type BlockPresenceManager struct {
	sync.RWMutex
	presence map[cid.Cid]map[peer.ID]bool
}

func New() *BlockPresenceManager {
	return &BlockPresenceManager{
		presence: make(map[cid.Cid]map[peer.ID]bool),
	}
}

func (bpm *BlockPresenceManager) ReceiveFrom(p peer.ID, haves []cid.Cid, dontHaves []cid.Cid) {
	bpm.Lock()
	defer bpm.Unlock()

	for _, c := range haves {
		bpm.updateBlockPresence(p, c, true)
	}
	for _, c := range dontHaves {
		bpm.updateBlockPresence(p, c, false)
	}
}

func (bpm *BlockPresenceManager) updateBlockPresence(p peer.ID, c cid.Cid, present bool) {
	_, ok := bpm.presence[c]
	if !ok {
		bpm.presence[c] = make(map[peer.ID]bool)
	}

	// Make sure not to change HAVE to DONT_HAVE
	has, pok := bpm.presence[c][p]
	if pok && has {
		return
	}
	bpm.presence[c][p] = present
}

func (bpm *BlockPresenceManager) PeerHasBlock(p peer.ID, c cid.Cid) bool {
	bpm.RLock()
	defer bpm.RUnlock()

	if ps, cok := bpm.presence[c]; cok {
		if has, pok := ps[p]; pok && has {
			return true
		}
	}
	return false
}

func (bpm *BlockPresenceManager) PeerDoesNotHaveBlock(p peer.ID, c cid.Cid) bool {
	bpm.RLock()
	defer bpm.RUnlock()

	if ps, cok := bpm.presence[c]; cok {
		if has, pok := ps[p]; pok && !has {
			return true
		}
	}
	return false
}

func (bpm *BlockPresenceManager) RemoveKeys(ks []cid.Cid) {
	bpm.RLock()
	defer bpm.RUnlock()

	for _, c := range ks {
		delete(bpm.presence, c)
	}
}
