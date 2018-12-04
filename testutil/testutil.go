package testutil

import (
	bsmsg "github.com/ipfs/go-bitswap/message"
	"github.com/ipfs/go-bitswap/wantlist"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	peer "github.com/libp2p/go-libp2p-peer"
)

var blockGenerator = blocksutil.NewBlockGenerator()
var prioritySeq int

// GenerateCids produces n content identifiers
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

// GenerateEntries makes fake bitswap message entries
func GenerateEntries(n int, isCancel bool) []*bsmsg.Entry {
	bsmsgs := make([]*bsmsg.Entry, 0, n)
	for i := 0; i < n; i++ {
		prioritySeq++
		msg := &bsmsg.Entry{
			Entry:  wantlist.NewRefEntry(blockGenerator.Next().Cid(), prioritySeq),
			Cancel: isCancel,
		}
		bsmsgs = append(bsmsgs, msg)
	}
	return bsmsgs
}

var peerSeq int

// GeneratePeers creates n peer ids
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(peerSeq)
		peerIds = append(peerIds, p)
	}
	return peerIds
}

var nextSession uint64

// GenerateSessionID make a unit session identifier
func GenerateSessionID() uint64 {
	nextSession++
	return uint64(nextSession)
}

// ContainsPeer returns true if a peer is found n a list of peers
func ContainsPeer(peers []peer.ID, p peer.ID) bool {
	for _, n := range peers {
		if p == n {
			return true
		}
	}
	return false
}
