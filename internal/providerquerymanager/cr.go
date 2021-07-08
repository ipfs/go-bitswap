package providerquerymanager

import (
	"context"

	crapi "github.com/libp2p/go-composable-routing/api"
	"github.com/libp2p/go-routing-language/parse"
	"github.com/libp2p/go-routing-language/patterns"
	"github.com/libp2p/go-routing-language/syntax"
)

// ProviderQueryManagerAsRouter converts a ProviderQueryManager to a Router,
// which is capable of evaluating find-cid queries.
func ProviderQueryManagerAsRouter(pqm *ProviderQueryManager) crapi.Router {
	return &providerQueryManagerAsRouter{pqm: pqm}
}

type providerQueryManagerAsRouter struct {
	pqm *ProviderQueryManager
}

func (x *providerQueryManagerAsRouter) Route(ctx context.Context, query syntax.Node) (<-chan syntax.Node, error) {
	// parse the routing query as a find-cid pattern
	findCid, err := patterns.ParseFindCid(parse.NewParseCtx(), query)
	if err != nil {
		// if the query is not a find-cid, this system does not support it.
		// hence, return the query unchanged.
		r := make(chan syntax.Node, 1)
		r <- query
		close(r)
		return r, nil
	}
	// otherwise, invoke the underlying PQM and rewrite the results as routing expressions
	pch := x.pqm.FindProvidersAsync(ctx, findCid.Cid)
	r := make(chan syntax.Node)
	go func() {
		defer close(r)
		for peerID := range pch {
			p := &patterns.FetchCid{
				Cid: findCid.Cid,
				Providers: patterns.Providers{
					&patterns.Peer{ID: peerID},
				},
			}
			r <- p.Express()
		}
	}()
	return r, nil
}
