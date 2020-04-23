package sessionmanager

import (
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	notifications "github.com/ipfs/go-bitswap/internal/notifications"
	bssession "github.com/ipfs/go-bitswap/internal/session"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Session is a session that is managed by the session manager
type Session interface {
	exchange.Fetcher
	ID() exchange.SessionID
	ReceiveFrom(peer.ID, []cid.Cid, []cid.Cid, []cid.Cid)
}

// SessionFactory generates a new session for the SessionManager to track.
type SessionFactory func(ctx context.Context, id exchange.SessionID, sprm bssession.SessionPeerManager, sim *bssim.SessionInterestManager, pm bssession.PeerManager, bpm *bsbpm.BlockPresenceManager, notif notifications.PubSub, provSearchDelay time.Duration, rebroadcastDelay delay.D, self peer.ID) Session

// PeerManagerFactory generates a new peer manager for a session.
type PeerManagerFactory func(ctx context.Context, id exchange.SessionID) bssession.SessionPeerManager

// SessionManager is responsible for creating, managing, and dispatching to
// sessions.
type SessionManager struct {
	ctx                    context.Context
	sessionFactory         SessionFactory
	sessionInterestManager *bssim.SessionInterestManager
	peerManagerFactory     PeerManagerFactory
	blockPresenceManager   *bsbpm.BlockPresenceManager
	peerManager            bssession.PeerManager
	notif                  notifications.PubSub

	// Sessions
	sessLk   sync.RWMutex
	sessions map[exchange.SessionID]Session

	self peer.ID
}

// New creates a new SessionManager.
func New(ctx context.Context, sessionFactory SessionFactory, sessionInterestManager *bssim.SessionInterestManager, peerManagerFactory PeerManagerFactory,
	blockPresenceManager *bsbpm.BlockPresenceManager, peerManager bssession.PeerManager, notif notifications.PubSub, self peer.ID) *SessionManager {
	return &SessionManager{
		ctx:                    ctx,
		sessionFactory:         sessionFactory,
		sessionInterestManager: sessionInterestManager,
		peerManagerFactory:     peerManagerFactory,
		blockPresenceManager:   blockPresenceManager,
		peerManager:            peerManager,
		notif:                  notif,
		sessions:               make(map[exchange.SessionID]Session),
		self:                   self,
	}
}

// GetSession gets the session associated with the context, or creates a new
// one.
func (sm *SessionManager) GetSession(ctx context.Context,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D) exchange.Fetcher {

	id, sessionctx := exchange.GetOrCreateSession(ctx)

	sm.sessLk.RLock()
	s, ok := sm.sessions[id]
	sm.sessLk.RUnlock()

	if ok {
		return s
	}

	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	if s, ok := sm.sessions[id]; ok {
		return s
	}

	sessionctx, cancel := context.WithCancel(sessionctx)

	pm := sm.peerManagerFactory(sessionctx, id)
	session := sm.sessionFactory(
		sessionctx, id, pm,
		sm.sessionInterestManager, sm.peerManager, sm.blockPresenceManager,
		sm.notif, provSearchDelay, rebroadcastDelay, sm.self,
	)
	sm.sessions[id] = session

	go func() {
		defer cancel()
		select {
		case <-sm.ctx.Done():
		case <-sessionctx.Done():
		}
		sm.removeSession(id)
	}()

	return session
}

func (sm *SessionManager) removeSession(sesid exchange.SessionID) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	delete(sm.sessions, sesid)
}

func (sm *SessionManager) ReceiveFrom(p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []Session {
	sessions := make([]Session, 0)

	// Notify each session that is interested in the blocks / HAVEs / DONT_HAVEs
	for _, id := range sm.sessionInterestManager.InterestedSessions(blks, haves, dontHaves) {
		sm.sessLk.RLock()
		sess, ok := sm.sessions[id]
		sm.sessLk.RUnlock()

		if ok {
			sess.ReceiveFrom(p, blks, haves, dontHaves)
			sessions = append(sessions, sess)
		}
	}

	return sessions
}
