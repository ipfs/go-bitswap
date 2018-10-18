package provide

type Session interface {
	Add(id cid.Cid, opts ...Option) bool
}

type Op interface {
	Refs(cid.Cid)
	IsRoot()
}

type Option func(Op)

func Refs(refs ...cid) Option {
	return func(op Op) {
		for _, ref := range refs {
			op.Refs(ref)
		}
	}
}

func Root(op Op) {
	op.IsRoot()
}

