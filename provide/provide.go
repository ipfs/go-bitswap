package provide

import "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"

type Session interface {
	Add(id cid.Cid, opts ...Option) bool
}

type Op interface {
	Refs(cid.Cid)
	IsRoot()
}

type Option func(Op)

func Refs(refs ...cid.Cid) Option {
	return func(op Op) {
		for _, ref := range refs {
			op.Refs(ref)
		}
	}
}

func Root(op Op) {
	op.IsRoot()
}
