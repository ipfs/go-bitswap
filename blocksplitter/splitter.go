package blocksplitter

import (
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multihash"
	"sync"

	pb "github.com/ipfs/go-bitswap/message/pb"
)

type BlockSplitter interface {
	GetManifest(block blocks.Block) (*pb.Message_BlockManifest, error)
	NewManifestVerifier(mh multihash.Multihash) (ManifestVerifier, error)
}

type ManifestVerifier interface {
	AddBytes(entry *VerifierEntry) ([]*VerifierEntry, []bool, error)
}

type splitterRegister struct {
	m  map[uint64]BlockSplitter
	lk sync.RWMutex
}

var splitterRegistration *splitterRegister

func init() {
	splitterRegistration = &splitterRegister{
		m: make(map[uint64]BlockSplitter),
	}
	if err := Register(multihash.SHA2_256, Sha256Splitter(1<<20)); err != nil {
		panic(err)
	}
}

func Register(hashCode uint64, splitter BlockSplitter) error {
	splitterRegistration.lk.Lock()
	defer splitterRegistration.lk.Unlock()
	if _, ok := splitterRegistration.m[hashCode]; ok {
		return fmt.Errorf("cannot register a block splitter for hash code %d as it is already registered", hashCode)
	}
	splitterRegistration.m[hashCode] = splitter
	return nil
}

func GetBlockSplitter(hashCode uint64) BlockSplitter {
	splitterRegistration.lk.RLock()
	defer splitterRegistration.lk.RUnlock()
	return splitterRegistration.m[hashCode]
}

func GetManifest(block blocks.Block) (*pb.Message_BlockManifest, error) {
	dec, err := multihash.Decode(block.Cid().Hash())
	if err != nil {
		return nil, err
	}
	bs := GetBlockSplitter(dec.Code)
	return bs.GetManifest(block)
}
