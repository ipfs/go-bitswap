package sessionmanager

import (
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	notifications "github.com/ipfs/go-bitswap/notifications"
	bssession "github.com/ipfs/go-bitswap/session"
	bssim "github.com/ipfs/go-bitswap/sessioninterestmanager"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Session is a session that is managed by the session manager
type Session interface {
	exchange.Fetcher
	ID() uint64
	ReceiveFrom(peer.ID, []cid.Cid, []cid.Cid, []cid.Cid)
}

type sesTrk struct {
	session Session
	pm      bssession.SessionPeerManager
	srs     bssession.RequestSplitter
}

// SessionFactory generates a new session for the SessionManager to track.
type SessionFactory func(ctx context.Context, id uint64, sprm bssession.SessionPeerManager, srs bssession.RequestSplitter, sim *bssim.SessionInterestManager, pm bssession.PeerManager, bpm *bsbpm.BlockPresenceManager, notif notifications.PubSub, provSearchDelay time.Duration, rebroadcastDelay delay.D, self peer.ID) Session

// RequestSplitterFactory generates a new request splitter for a session.
type RequestSplitterFactory func(ctx context.Context) bssession.RequestSplitter

// PeerManagerFactory generates a new peer manager for a session.
type PeerManagerFactory func(ctx context.Context, id uint64) bssession.SessionPeerManager

// SessionManager is responsible for creating, managing, and dispatching to
// sessions.
type SessionManager struct {
	ctx                    context.Context
	sessionFactory         SessionFactory
	sessionInterestManager *bssim.SessionInterestManager
	peerManagerFactory     PeerManagerFactory
	blockPresenceManager   *bsbpm.BlockPresenceManager
	requestSplitterFactory RequestSplitterFactory
	peerManager            bssession.PeerManager
	notif                  notifications.PubSub

	// Sessions
	sessLk   sync.RWMutex
	sessions map[uint64]sesTrk

	// Session Index
	sessIDLk sync.Mutex
	sessID   uint64

	self peer.ID
}

// New creates a new SessionManager.
func New(ctx context.Context, sessionFactory SessionFactory, sessionInterestManager *bssim.SessionInterestManager, peerManagerFactory PeerManagerFactory,
	blockPresenceManager *bsbpm.BlockPresenceManager, requestSplitterFactory RequestSplitterFactory, peerManager bssession.PeerManager, notif notifications.PubSub, self peer.ID) *SessionManager {
	return &SessionManager{
		ctx:                    ctx,
		sessionFactory:         sessionFactory,
		sessionInterestManager: sessionInterestManager,
		peerManagerFactory:     peerManagerFactory,
		blockPresenceManager:   blockPresenceManager,
		requestSplitterFactory: requestSplitterFactory,
		peerManager:            peerManager,
		notif:                  notif,
		sessions:               make(map[uint64]sesTrk),
		self:                   self,
	}
}

// NewSession initializes a session with the given context, and adds to the
// session manager.
func (sm *SessionManager) NewSession(ctx context.Context,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D) exchange.Fetcher {
	id := sm.GetNextSessionID()
	sessionctx, cancel := context.WithCancel(ctx)

	pm := sm.peerManagerFactory(sessionctx, id)
	srs := sm.requestSplitterFactory(sessionctx)
	session := sm.sessionFactory(sessionctx, id, pm, srs, sm.sessionInterestManager, sm.peerManager, sm.blockPresenceManager, sm.notif, provSearchDelay, rebroadcastDelay, sm.self)
	tracked := sesTrk{session, pm, srs}
	sm.sessLk.Lock()
	sm.sessions[id] = tracked
	sm.sessLk.Unlock()
	go func() {
		defer cancel()
		select {
		case <-sm.ctx.Done():
			sm.removeSession(tracked)
		case <-ctx.Done():
			sm.removeSession(tracked)
		}
	}()

	return session
}

func (sm *SessionManager) removeSession(st sesTrk) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	delete(sm.sessions, st.session.ID())
}

// GetNextSessionID returns the next sequential identifier for a session.
func (sm *SessionManager) GetNextSessionID() uint64 {
	sm.sessIDLk.Lock()
	defer sm.sessIDLk.Unlock()

	sm.sessID++
	return sm.sessID
}

func (sm *SessionManager) ReceiveFrom(p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []Session {
	sessions := make([]Session, 0)

	// Notify each session that is interested in the blocks / HAVEs / DONT_HAVEs
	for _, id := range sm.sessionInterestManager.InterestedSessions(blks, haves, dontHaves) {
		sm.sessLk.RLock()
		sess, ok := sm.sessions[id]
		sm.sessLk.RUnlock()

		if ok {
			sess.session.ReceiveFrom(p, blks, haves, dontHaves)
			sessions = append(sessions, sess.session)
		}
	}

	return sessions
}
