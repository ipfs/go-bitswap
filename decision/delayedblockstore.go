package decision

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
)

type delayedBlockstore struct {
	bs    bstore.Blockstore
	delay delay.D
}

func newDelayedBlockstore(bs bstore.Blockstore, delay delay.D) bstore.Blockstore {
	return &delayedBlockstore{bs: bs, delay: delay}
}

func (dbs *delayedBlockstore) DeleteBlock(c cid.Cid) error {
	dbs.delay.Wait()
	return dbs.bs.DeleteBlock(c)
}

func (dbs *delayedBlockstore) Has(c cid.Cid) (bool, error) {
	dbs.delay.Wait()
	return dbs.bs.Has(c)
}
func (dbs *delayedBlockstore) Get(c cid.Cid) (blocks.Block, error) {
	dbs.delay.Wait()
	return dbs.bs.Get(c)
}

func (dbs *delayedBlockstore) GetSize(c cid.Cid) (int, error) {
	dbs.delay.Wait()
	return dbs.bs.GetSize(c)
}

func (dbs *delayedBlockstore) Put(b blocks.Block) error {
	dbs.delay.Wait()
	return dbs.bs.Put(b)
}

func (dbs *delayedBlockstore) PutMany(blks []blocks.Block) error {
	dbs.delay.Wait()
	return dbs.bs.PutMany(blks)
}

func (dbs *delayedBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	dbs.delay.Wait()
	return dbs.bs.AllKeysChan(ctx)
}

func (dbs *delayedBlockstore) HashOnRead(enabled bool) {
	dbs.bs.HashOnRead(enabled)
}
