package bitswap

import (
	"context"

	bsnet "github.com/ipfs/go-bitswap/network"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	opts "github.com/libp2p/go-libp2p-kad-dht/opts"
	record "github.com/libp2p/go-libp2p-record"
	tnet "github.com/libp2p/go-libp2p-testing/net"

	ds "github.com/ipfs/go-datastore"

	"github.com/libp2p/go-libp2p-core/peer"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// RoutingServer can generate routing clients from a host and an indentity
type RoutingServer interface {
	Client(ctx context.Context, p2pHost host.Host, p tnet.Identity) routing.Routing
}

// MockServer returns clients that use mock routing.
type MockServer struct {
	rs mockrouting.Server
}

// NewMockServer returns a MockServer.
func NewMockServer() RoutingServer {
	return &MockServer{rs: mockrouting.NewServer()}
}

// Client returns a new mock routing instance for the given host and identity.
func (ms *MockServer) Client(ctx context.Context, p2pHost host.Host, p tnet.Identity) routing.Routing {
	return ms.rs.ClientWithDatastore(ctx, p, ds.NewMapDatastore())
}

// DHTServer generates real Kademlia DHT routing instances.
type DHTServer struct {
}

// NewDHTServer generates a new DHT based routing server
func NewDHTServer() RoutingServer {
	return &DHTServer{}
}

// Client returns a new Kademlia DHT Routing interface
func (dhtServer *DHTServer) Client(ctx context.Context, p2pHost host.Host, p tnet.Identity) routing.Routing {
	routing, err := dht.New(
		ctx, p2pHost,
		opts.Datastore(ds.NewMapDatastore()),
		opts.Validator(record.PublicKeyValidator{}),
	)
	if err != nil {
		panic(err.Error())
	}
	return routing
}

type peernet struct {
	mockpeernet.Mocknet
	routingserver RoutingServer
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet(ctx context.Context, net mockpeernet.Mocknet, rs RoutingServer) (Network, error) {
	return &peernet{net, rs}, nil
}

func (pn *peernet) Adapter(p tnet.Identity) bsnet.BitSwapNetwork {
	client, err := pn.Mocknet.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	routing := pn.routingserver.Client(context.TODO(), client, p)
	return bsnet.NewFromIpfsHost(client, routing)
}

func (pn *peernet) HasPeer(p peer.ID) bool {
	for _, member := range pn.Mocknet.Peers() {
		if p == member {
			return true
		}
	}
	return false
}

var _ Network = (*peernet)(nil)
