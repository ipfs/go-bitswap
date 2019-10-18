package session

import (
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type logger struct {
	self peer.ID
	lg   logging.EventLogger
}

func newLogger(self peer.ID) *logger {
	return &logger{
		self: self,
		lg:   logging.Logger("bs:spm"),
	}
}

func (l *logger) log(str string, args ...interface{}) {
	// str = fmt.Sprintf("%s: %s", lu.P(l.self), str)
	// l.lg.Warningf(str, args...)
}
