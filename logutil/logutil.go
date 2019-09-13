package logutil

import (
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func C(c cid.Cid) string {
	if c.Defined() {
		str := c.String()
		return str[len(str)-6:]
	}
	return "<undef cid>"
}

func P(p peer.ID) string {
	if p != "" {
		str := p.String()
		return str[len(str)-6:]
	}
	return "<undef peer>"
}