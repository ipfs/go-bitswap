package decision

import (
	"sync"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"

	pb "github.com/daotl/go-bitswap/message/pb"
	wl "github.com/daotl/go-bitswap/wantlist"
)

func newLedger(p peer.ID) *ledger {
	return &ledger{
		wantList: wl.New(),
		Partner:  p,
	}
}

// Keeps the wantlist for the partner. NOT threadsafe!
type ledger struct {
	// Partner is the remote Peer.
	Partner peer.ID

	// wantList is a (bounded, small) set of keys that Partner desires.
	wantList *wl.Wantlist

	lk sync.RWMutex
}

func (l *ledger) Wants(k cid.Cid, priority int32, wantType pb.Message_Wantlist_WantType) {
	log.Debugf("peer %s wants %s", l.Partner, k)
	l.wantList.Add(k, priority, wantType)
}

func (l *ledger) CancelWant(k cid.Cid) bool {
	return l.wantList.Remove(k)
}

func (l *ledger) WantListContains(k cid.Cid) (wl.Entry, bool) {
	return l.wantList.Contains(k)
}
