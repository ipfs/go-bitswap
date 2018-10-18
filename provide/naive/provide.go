package naive

import (
	"github.com/ipfs/go-bitswap/provide"
	"gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
)

type Session struct{}

func (Session) Add(cid.Cid, ...provide.Option) bool {
	return true
}

type Op struct{}

func (Op) Refs(cid.Cid) {}
func (Op) IsRoot()      {}
