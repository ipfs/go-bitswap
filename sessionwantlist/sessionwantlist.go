package sessionwantlist

import (
	"sync"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("bs:bcst")

type SessionWantlist struct {
	sync.RWMutex
	wants map[cid.Cid]map[uint64]struct{}
}

func NewSessionWantlist() *SessionWantlist {
	return &SessionWantlist{
		wants: make(map[cid.Cid]map[uint64]struct{}),
	}
}

func (swl *SessionWantlist) Add(ks []cid.Cid, ses uint64) {
	swl.Lock()
	defer swl.Unlock()

	for _, c := range ks {
		if _, ok := swl.wants[c]; !ok {
			swl.wants[c] = make(map[uint64]struct{})
		}
		swl.wants[c][ses] = struct{}{}
	}
}

func (swl *SessionWantlist) RemoveKeys(ks []cid.Cid) {
	swl.Lock()
	defer swl.Unlock()

	for _, c := range ks {
		delete(swl.wants, c)
	}
}

func (swl *SessionWantlist) RemoveSession(ses uint64) []cid.Cid {
	swl.Lock()
	defer swl.Unlock()

	deletedKs := make([]cid.Cid, 0)
	for c := range swl.wants {
		delete(swl.wants[c], ses)
		if len(swl.wants[c]) == 0 {
			delete(swl.wants, c)
			deletedKs = append(deletedKs, c)
		}
	}

	return deletedKs
}

func (swl *SessionWantlist) Keys() []cid.Cid {
	swl.RLock()
	defer swl.RUnlock()

	ks := make([]cid.Cid, 0, len(swl.wants))
	for c := range swl.wants {
		ks = append(ks, c)
	}
	return ks
}

func (swl *SessionWantlist) SessionsFor(ks []cid.Cid) []uint64 {
	swl.RLock()
	defer swl.RUnlock()

	sesMap := make(map[uint64]struct{})
	for _, c := range ks {
		for s := range swl.wants[c] {
			sesMap[s] = struct{}{}
		}
	}

	ses := make([]uint64, 0, len(sesMap))
	for s := range sesMap {
		ses = append(ses, s)
	}
	return ses
}
