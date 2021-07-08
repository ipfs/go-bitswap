package session

import (
	"context"

	cid "github.com/ipfs/go-cid"
	crapi "github.com/libp2p/go-composable-routing/api"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-routing-language/parse"
	"github.com/libp2p/go-routing-language/patterns"
)

// RouterAsProviderFinder converts a composable Router to a ProviderFinder.
func RouterAsProviderFinder(router crapi.Router) ProviderFinder {
	return &routerAsProviderFinder{router}
}

type routerAsProviderFinder struct {
	router crapi.Router
}

func (x *routerAsProviderFinder) FindProvidersAsync(ctx context.Context, k cid.Cid) <-chan peer.ID {
	p := &patterns.FindCid{Cid: k}
	rch, err := x.router.Route(ctx, p.Express())
	if err != nil {
		log.Errorf("composable route (%v)", err)
		pch := make(chan peer.ID)
		close(pch)
		return pch
	}
	pch := make(chan peer.ID)
	go func() {
		defer close(pch)
		pctx := parse.NewParseCtx()
		for r := range rch {
			fetchCid, err := patterns.ParseFetchCid(pctx, r)
			if err != nil {
				log.Infof("ignoring non fetch-cid expression: %v", r)
			}
			for _, prov := range fetchCid.Providers {
				peer, ok := prov.(*patterns.Peer)
				if !ok {
					log.Infof("ignoring non-peer provider: %v", p)
				}
				pch <- peer.ID
			}
		}
	}()
	return pch
}
