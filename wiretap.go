package bitswap

import (
	peer "github.com/libp2p/go-libp2p-core/peer"

	bsmsg "github.com/daotl/go-bitswap/message"
)

// WireTap provides methods to access all messages sent and received by Bitswap.
// This interface can be used to implement various statistics (this is original intent).
type WireTap interface {
	MessageReceived(peer.ID, bsmsg.BitSwapMessage)
	MessageSent(peer.ID, bsmsg.BitSwapMessage)
}

// Configures Bitswap to use given wiretap.
func EnableWireTap(tap WireTap) Option {
	return func(bs *Bitswap) {
		bs.wiretap = tap
	}
}

// Configures Bitswap not to use any wiretap.
func DisableWireTap() Option {
	return func(bs *Bitswap) {
		bs.wiretap = nil
	}
}
