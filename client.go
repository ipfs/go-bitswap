package bitswap

import (
	"context"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	"github.com/ipfs/go-bitswap/internal/defaults"
	bsgetter "github.com/ipfs/go-bitswap/internal/getter"
	bsmq "github.com/ipfs/go-bitswap/internal/messagequeue"
	"github.com/ipfs/go-bitswap/internal/notifications"
	"github.com/ipfs/go-bitswap/internal/peermanager"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	bspqm "github.com/ipfs/go-bitswap/internal/providerquerymanager"
	bssession "github.com/ipfs/go-bitswap/internal/session"
	"github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	"github.com/ipfs/go-bitswap/internal/sessionmanager"
	bsspm "github.com/ipfs/go-bitswap/internal/sessionpeermanager"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	process "github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p-core/peer"
)

type client struct {
	process process.Process
	pqm     *bspqm.ProviderQueryManager
	sm      *sessionmanager.SessionManager
	sim     *sessioninterestmanager.SessionInterestManager
	pm      *peermanager.PeerManager
	bs      blockstore.Blockstore
	notif   notifications.PubSub

	provSearchDelay  time.Duration
	rebroadcastDelay delay.D

	// onAddedBlocks is called after new blocks are received and added to the blockstore.
	// If there's a server, then this can notify it when new blocks have been added,
	// so it can check if they are wanted by other peers and send them the new blocks.
	onAddedBlocks func(context.Context, peer.ID, []blocks.Block)

	cancelFn func()
	close    chan struct{}
}

type clientOpts struct {
	provSearchDelay            time.Duration
	rebroadcastDelay           delay.D
	simulateDontHavesOnTimeout bool
}

type ClientOption func(*clientOpts)

func WithClientProviderSearchDelay(d time.Duration) ClientOption {
	return func(o *clientOpts) {
		o.provSearchDelay = d
	}
}

func WithClientRebroadcastDelay(d delay.D) ClientOption {
	return func(o *clientOpts) {
		o.rebroadcastDelay = d
	}
}

// NewClient constructs a new Bitswap client.
// onAddedBlocks is invoked after received blocks are added to the blockstore.
func NewClient(px process.Process, bs blockstore.Blockstore, network bsnet.BitSwapNetwork, onAddedBlocks func(context.Context, peer.ID, []blocks.Block), opts ...ClientOption) *client {
	clientOpts := &clientOpts{
		provSearchDelay:            defaults.ProvSearchDelay,
		rebroadcastDelay:           delay.Fixed(time.Minute),
		simulateDontHavesOnTimeout: true,
	}
	for _, o := range opts {
		o(clientOpts)
	}

	// TODO: remove this, it's just to ease refactoring
	ctx, cancelFn := context.WithCancel(context.Background())

	var sm *sessionmanager.SessionManager

	onDontHaveTimeout := func(p peer.ID, dontHaves []cid.Cid) {
		// Simulate a message arriving with DONT_HAVEs
		if clientOpts.simulateDontHavesOnTimeout {
			sm.ReceiveFrom(ctx, p, nil, nil, dontHaves)
		}
	}
	peerQueueFactory := func(ctx context.Context, p peer.ID) bspm.PeerQueue {
		return bsmq.New(ctx, p, network, onDontHaveTimeout)
	}

	sim := bssim.New()
	bpm := bsbpm.New()
	pm := bspm.New(ctx, peerQueueFactory, network.Self())
	pqm := bspqm.New(ctx, network)

	sessionFactory := func(
		sessctx context.Context,
		sessmgr bssession.SessionManager,
		id uint64,
		spm bssession.SessionPeerManager,
		sim *bssim.SessionInterestManager,
		pm bssession.PeerManager,
		bpm *bsbpm.BlockPresenceManager,
		notif notifications.PubSub,
		provSearchDelay time.Duration,
		rebroadcastDelay delay.D,
		self peer.ID) sessionmanager.Session {
		return bssession.New(sessctx, sessmgr, id, spm, pqm, sim, pm, bpm, notif, provSearchDelay, rebroadcastDelay, self)
	}
	sessionPeerManagerFactory := func(ctx context.Context, id uint64) bssession.SessionPeerManager {
		return bsspm.New(id, network.ConnectionManager())
	}
	notif := notifications.New()
	sm = sessionmanager.New(ctx, sessionFactory, sim, sessionPeerManagerFactory, bpm, pm, notif, network.Self())

	c := &client{
		process:          px,
		pqm:              pqm,
		sm:               sm,
		bs:               bs,
		onAddedBlocks:    onAddedBlocks,
		provSearchDelay:  clientOpts.provSearchDelay,
		rebroadcastDelay: clientOpts.rebroadcastDelay,

		cancelFn: cancelFn,
		close:    make(chan struct{}),
	}

	c.pqm.Startup()

	go func() {
		<-px.Closing()
		cancelFn()
		sm.Shutdown()
		notif.Shutdown()
	}()

	return c

}

func (s *client) ReceiveMessage(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	blks := m.Blocks()
	haves := m.Haves()
	donthaves := m.DontHaves()
	if len(blks) > 0 || len(haves) > 0 || len(donthaves) > 0 {
		err := s.ReceivedBlocksFrom(ctx, p, blks, haves, donthaves)
		if err != nil {
			log.Warnf("ReceiveMessage recvBlockFrom error: %s", err)
		}
	}
}

func (s *client) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	session := s.sm.NewSession(ctx, s.provSearchDelay, s.rebroadcastDelay)
	return session.GetBlocks(ctx, keys)
}

func (s *client) GetBlock(ctx context.Context, key cid.Cid) (blocks.Block, error) {
	return bsgetter.SyncGetBlock(ctx, key, s.GetBlocks)
}

func (s *client) PeerConnected(p peer.ID) {
	s.pm.Connected(p)
}
func (s *client) PeerDisconnected(p peer.ID) {
	s.pm.Disconnected(p)
}

func (s *client) Shutdown() {
	s.cancelFn()
	s.sm.Shutdown()
}

func (s *client) NewSession(ctx context.Context) exchange.Fetcher {
	return s.sm.NewSession(ctx, s.provSearchDelay, s.rebroadcastDelay)
}

func (s *client) GetWantlist() []cid.Cid {
	return s.pm.CurrentWants()
}

func (s *client) GetWantBlocks() []cid.Cid {
	return s.pm.CurrentWantBlocks()
}

func (s *client) GetWantHaves() []cid.Cid {
	return s.pm.CurrentWantHaves()
}

func (s *client) ReceivedBlocksFrom(ctx context.Context, from peer.ID, blks []blocks.Block, haves []cid.Cid, dontHaves []cid.Cid) error {
	wanted := blks

	// If blocks came from the network
	if from != "" {
		var notWanted []blocks.Block
		wanted, notWanted = s.sim.SplitWantedUnwanted(blks)
		for _, b := range notWanted {
			log.Debugf("[recv] block not in wantlist; cid=%s, peer=%s", b.Cid(), from)
		}
	}

	// Put wanted blocks into blockstore
	if len(wanted) > 0 {
		err := s.bs.PutMany(ctx, wanted)
		if err != nil {
			log.Errorf("Error writing %d blocks to datastore: %s", len(wanted), err)
			return err
		}
	}

	// NOTE: There exists the possiblity for a race condition here.  If a user
	// creates a node, then adds it to the dagservice while another goroutine
	// is waiting on a GetBlock for that object, they will receive a reference
	// to the same node. We should address this soon, but i'm not going to do
	// it now as it requires more thought and isnt causing immediate problems.

	allKs := make([]cid.Cid, 0, len(blks))
	for _, b := range blks {
		allKs = append(allKs, b.Cid())
	}

	// If the message came from the network
	if from != "" {
		// Inform the PeerManager so that we can calculate per-peer latency
		combined := make([]cid.Cid, 0, len(allKs)+len(haves)+len(dontHaves))
		combined = append(combined, allKs...)
		combined = append(combined, haves...)
		combined = append(combined, dontHaves...)
		s.pm.ResponseReceived(from, combined)
	}

	// Send all block keys (including duplicates) to any sessions that want them.
	// (The duplicates are needed by sessions for accounting purposes)
	s.sm.ReceiveFrom(ctx, from, allKs, haves, dontHaves)

	s.onAddedBlocks(ctx, from, wanted)

	// Publish the block to any Bitswap clients that had requested blocks.
	// (the sessions use this pubsub mechanism to inform clients of incoming
	// blocks)
	for _, b := range wanted {
		s.notif.Publish(b)
	}

	if from != "" {
		for _, b := range wanted {
			log.Debugw("Bitswap.GetBlockRequest.End", "cid", b.Cid())
		}
	}

	return nil
}
