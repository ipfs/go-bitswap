package peermanager

import (
	"bytes"
	"fmt"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// peerWantManager
type peerWantManager struct {
	peerWants map[peer.ID]*peerWant
}

type peerWant struct {
	wantBlocks *cid.Set
	wantHaves  *cid.Set
}

// New creates a new peerWantManager.
func newPeerWantManager() *peerWantManager {
	return &peerWantManager{
		peerWants: make(map[peer.ID]*peerWant),
	}
}

func (pwm *peerWantManager) AddPeer(p peer.ID) {
	if _, ok := pwm.peerWants[p]; !ok {
		pwm.peerWants[p] = &peerWant{
			wantBlocks: cid.NewSet(),
			wantHaves:  cid.NewSet(),
		}
	}
}

func (pwm *peerWantManager) RemovePeer(p peer.ID) {
	// TODO: does this leak memory for cid sets?
	delete(pwm.peerWants, p)
}

func (pwm *peerWantManager) BroadcastWantHaves(wantHaves []cid.Cid) map[peer.ID][]cid.Cid {
	res := make(map[peer.ID][]cid.Cid)

	// Iterate over all known peers
	for p, pws := range pwm.peerWants {
		// Iterate over all want-haves
		for _, c := range wantHaves {
			// If the CID has not been sent as a want-block or want-have
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				// Record that the CID has been sent as a want-have
				pws.wantHaves.Add(c)

				// Add the CID to the results
				if _, ok := res[p]; !ok {
					res[p] = make([]cid.Cid, 0, 1)
				}
				res[p] = append(res[p], c)
			}
		}
	}

	return res
}

func (pwm *peerWantManager) SendWants(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) ([]cid.Cid, []cid.Cid) {
	resWantBlks := make([]cid.Cid, 0)
	resWantHvs := make([]cid.Cid, 0)

	// Get the existing want-blocks and want-haves for the peer
	if pws, ok := pwm.peerWants[p]; ok {
		// Iterate over the requested want-blocks
		for _, c := range wantBlocks {
			// If the want-block hasn't been sent to the peer
			if !pws.wantBlocks.Has(c) {
				// Record that the CID was sent as a want-block
				pws.wantBlocks.Add(c)

				// Add the CID to the results
				resWantBlks = append(resWantBlks, c)

				// Make sure the CID is no longer recorded as a want-have
				pws.wantHaves.Remove(c)
			}
		}

		// Iterate over the requested want-haves
		for _, c := range wantHaves {
			// If the CID has not been sent as a want-block or want-have
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				// Record that the CID was sent as a want-have
				pws.wantHaves.Add(c)

				// Add the CID to the results
				resWantHvs = append(resWantHvs, c)
			}
		}
	}

	return resWantBlks, resWantHvs
}

func (pwm *peerWantManager) SendCancels(cancelKs []cid.Cid) map[peer.ID][]cid.Cid {
	res := make(map[peer.ID][]cid.Cid)

	// Iterate over all known peers
	for p, pws := range pwm.peerWants {
		// Iterate over all requested cancels
		for _, c := range cancelKs {
			// If the CID was sent as a want-block or want-have
			if pws.wantBlocks.Has(c) || pws.wantHaves.Has(c) {
				// Remove the CID from the recorded want-blocks and want-haves
				pws.wantBlocks.Remove(c)
				pws.wantHaves.Remove(c)

				// Add the CID to the results
				if _, ok := res[p]; !ok {
					res[p] = make([]cid.Cid, 0, 1)
				}
				res[p] = append(res[p], c)
			}
		}
	}

	return res
}

func (pwm *peerWantManager) PeerCanSendWants(p peer.ID, wants []cid.Cid) []cid.Cid {
	res := make([]cid.Cid, 0)

	// If the peer is connected
	if pws, ok := pwm.peerWants[p]; ok {
		// Iterate over requested wants
		for _, c := range wants {
			// If the CID has not been sent as a want-block or want-have
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				// Add the peer to the results
				res = append(res, c)
			}
		}
	}

	return res
}

func (pwm *peerWantManager) PeersCanSendWantBlock(c cid.Cid, peers []peer.ID) []peer.ID {
	res := make([]peer.ID, 0)

	// Iterate over requested peers
	for _, p := range peers {
		// If the peer is connected
		if pws, ok := pwm.peerWants[p]; ok {
			// If the CID has not been sent as a want-block
			// (it may have been sent as a want-have)
			if !pws.wantBlocks.Has(c) {
				// Add the peer to the results
				res = append(res, p)
			}
		}
	}

	return res
}

func (pwm *peerWantManager) GetWantBlocks() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-blocks
		for _, c := range pws.wantBlocks.Keys() {
			// Add the CID to the results
			res.Add(c)
		}
	}

	return res.Keys()
}

func (pwm *peerWantManager) String() string {
	var b bytes.Buffer
	for p, ws := range pwm.peerWants {
		b.WriteString(fmt.Sprintf("Peer %s: %d want-have / %d want-block:\n", p, ws.wantHaves.Len(), ws.wantBlocks.Len()))
		for _, c := range ws.wantHaves.Keys() {
			b.WriteString(fmt.Sprintf("  want-have  %s\n", c.String()[2:8]))
		}
		for _, c := range ws.wantBlocks.Keys() {
			b.WriteString(fmt.Sprintf("  want-block %s\n", c.String()[2:8]))
		}
	}
	return b.String()
}
