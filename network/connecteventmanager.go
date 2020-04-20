package network

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

type connectEvent int

const (
	connectEventConnected connectEvent = iota
	connectEventDisconnected
	connectEventMarkUnresponsive
	connectEventMessage
)

type connEvt struct {
	p       peer.ID
	evtType connectEvent
}

type ConnectionListener interface {
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

type connectEventManager struct {
	connListener ConnectionListener
	events       chan connEvt
	done         chan struct{}
}

func newConnectEventManager(connListener ConnectionListener) *connectEventManager {
	return &connectEventManager{
		connListener: connListener,
		events:       make(chan connEvt, 256),
		done:         make(chan struct{}),
	}
}

func (c *connectEventManager) Connected(p peer.ID) {
	c.events <- connEvt{p, connectEventConnected}
}

func (c *connectEventManager) Disconnected(p peer.ID) {
	c.events <- connEvt{p, connectEventDisconnected}
}

func (c *connectEventManager) MarkUnresponsive(p peer.ID) {
	c.events <- connEvt{p, connectEventMarkUnresponsive}
}

func (c *connectEventManager) OnMessage(p peer.ID) {
	c.events <- connEvt{p, connectEventMessage}
}

func (c *connectEventManager) Run() {
	gcTicker := time.NewTicker(30 * time.Second)
	defer gcTicker.Stop()

	peerEventsHandlers := make(map[peer.ID]*peerEventsHandler)
	defer func() {
		for _, h := range peerEventsHandlers {
			h.shutdown()
		}
	}()

	for {
		select {
		case evt := <-c.events:
			handler, ok := peerEventsHandlers[evt.p]
			if !ok {
				if evt.evtType != connectEventConnected {
					break
				}
				handler = newPeerEventsHandler(evt.p, c.connListener)
				peerEventsHandlers[evt.p] = handler
			}
			handler.add(evt.evtType)

		case <-gcTicker.C:
			for p, handler := range peerEventsHandlers {
				if handler.canGC() {
					delete(peerEventsHandlers, p)
				}
			}

		case <-c.done:
			return
		}
	}
}

func (c *connectEventManager) shutdown() {
	close(c.done)
}

type peerEventsHandler struct {
	p            peer.ID
	connListener ConnectionListener
	responsive   bool
	events       chan connectEvent
	done         chan struct{}

	refsLk sync.RWMutex
	refs   int

	eventCountLk sync.Mutex
	eventCount   int
}

func newPeerEventsHandler(p peer.ID, connListener ConnectionListener) *peerEventsHandler {
	return &peerEventsHandler{
		p:            p,
		connListener: connListener,
		responsive:   true,
		events:       make(chan connectEvent, 64),
		done:         make(chan struct{}),
	}
}

func (pe *peerEventsHandler) add(evtType connectEvent) {
	pe.eventCountLk.Lock()
	pe.eventCount++
	pe.eventCountLk.Unlock()

	pe.events <- evtType
}

func (pe *peerEventsHandler) run() {
	for {
		select {
		case evtType := <-pe.events:
			switch evtType {
			case connectEventConnected:
				pe.onConnected()
			case connectEventDisconnected:
				pe.onDisconnected()
			case connectEventMarkUnresponsive:
				pe.onMarkUnresponsive()
			case connectEventMessage:
				pe.onMessage()
			}

			pe.eventCountLk.Lock()
			pe.eventCount--
			pe.eventCountLk.Unlock()

		case <-pe.done:
			return
		}
	}
}

func (pe *peerEventsHandler) shutdown() {
	close(pe.done)
}

func (pe *peerEventsHandler) canGC() bool {
	pe.refsLk.RLock()
	defer pe.refsLk.RUnlock()

	return pe.refs <= 0
}

func (pe *peerEventsHandler) onConnected() {
	pe.refsLk.Lock()

	pe.refs++
	fireEvent := pe.refs == 1 && pe.responsive

	pe.refsLk.Unlock()

	if fireEvent {
		pe.connListener.PeerConnected(pe.p)
	}
}

func (pe *peerEventsHandler) onDisconnected() {
	pe.refsLk.Lock()

	pe.refs--
	fireEvent := pe.refs == 0 && pe.responsive

	pe.refsLk.Unlock()

	if fireEvent {
		pe.connListener.PeerDisconnected(pe.p)
	}
}

func (pe *peerEventsHandler) onMarkUnresponsive() {
	if !pe.responsive {
		return
	}

	pe.responsive = false
	pe.connListener.PeerDisconnected(pe.p)
}

func (pe *peerEventsHandler) onMessage() {
	if pe.responsive {
		return
	}

	pe.responsive = true
	pe.connListener.PeerConnected(pe.p)
}
