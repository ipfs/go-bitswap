package sessionmanager

import (
	"context"
	"sync"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bssession "github.com/ipfs/go-bitswap/internal/session"
	bswrm "github.com/ipfs/go-bitswap/internal/wantrequestmanager"
	wrm "github.com/ipfs/go-bitswap/internal/wantrequestmanager"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Session is a session that is managed by the session manager
type Session interface {
	exchange.Fetcher
	ID() uint64
	Shutdown()
}

// SessionFactory generates a new session for the SessionManager to track.
type SessionFactory func(
	ctx context.Context,
	sm bssession.SessionManager,
	id uint64,
	sprm bssession.SessionPeerManager,
	pm bssession.PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	wrm *bswrm.WantRequestManager,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D,
	self peer.ID) Session

// PeerManagerFactory generates a new peer manager for a session.
type PeerManagerFactory func(ctx context.Context, id uint64) bssession.SessionPeerManager

// SessionManager is responsible for creating, managing, and dispatching to
// sessions.
type SessionManager struct {
	sessionFactory       SessionFactory
	peerManagerFactory   PeerManagerFactory
	blockPresenceManager *bsbpm.BlockPresenceManager
	peerManager          bssession.PeerManager
	wrm                  *wrm.WantRequestManager

	// Sessions
	sessLk   sync.RWMutex
	sessions map[uint64]Session

	// Session Index
	sessIDLk sync.Mutex
	sessID   uint64

	self peer.ID
}

// New creates a new SessionManager.
func New(sessionFactory SessionFactory, peerManagerFactory PeerManagerFactory,
	blockPresenceManager *bsbpm.BlockPresenceManager, peerManager bssession.PeerManager, wrm *bswrm.WantRequestManager, self peer.ID) *SessionManager {

	return &SessionManager{
		sessionFactory:       sessionFactory,
		peerManagerFactory:   peerManagerFactory,
		blockPresenceManager: blockPresenceManager,
		peerManager:          peerManager,
		wrm:                  wrm,
		sessions:             make(map[uint64]Session),
		self:                 self,
	}
}

// NewSession initializes a session with the given context, and adds to the
// session manager.
func (sm *SessionManager) NewSession(ctx context.Context,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D) exchange.Fetcher {
	id := sm.GetNextSessionID()

	pm := sm.peerManagerFactory(ctx, id)
	session := sm.sessionFactory(ctx, sm, id, pm, sm.peerManager, sm.blockPresenceManager, sm.wrm, provSearchDelay, rebroadcastDelay, sm.self)

	sm.sessLk.Lock()
	if sm.sessions == nil { // check if SessionManager was shutdown
		session.Shutdown()
	} else {
		sm.sessions[id] = session
	}
	sm.sessLk.Unlock()

	return session
}

func (sm *SessionManager) Shutdown() {
	sm.sessLk.Lock()

	sessions := make([]Session, 0, len(sm.sessions))
	for _, ses := range sm.sessions {
		sessions = append(sessions, ses)
	}

	// Ensure that if Shutdown() is called twice we only shut down
	// the sessions once
	sm.sessions = nil

	sm.sessLk.Unlock()

	for _, ses := range sessions {
		ses.Shutdown()
	}
}

func (sm *SessionManager) RemoveSession(sesid uint64) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	// Clean up session
	if sm.sessions != nil { // check if SessionManager was shutdown
		delete(sm.sessions, sesid)
	}
}

// GetNextSessionID returns the next sequential identifier for a session.
func (sm *SessionManager) GetNextSessionID() uint64 {
	sm.sessIDLk.Lock()
	defer sm.sessIDLk.Unlock()

	sm.sessID++
	return sm.sessID
}
