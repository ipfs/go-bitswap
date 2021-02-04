package session

import (
	"bytes"
	"github.com/ipfs/go-bitswap/blocksplitter"
	"github.com/ipfs/go-bitswap/message"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"sync"
)

var lbslog = logging.Logger("bs:sess:largeblocksubsess")

// LargeBlockSubSession holds state for a large block sub-session
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
type LargeBlockSubSession struct {
	sessMx    sync.Mutex
	manifests map[peer.ID]message.LargeBlockManifest
	verifier  blocksplitter.ManifestVerifier

	mcid cid.Cid

	// TODO: datastore usage
	verifiedChunks []*blocksplitter.VerifierEntry
	resultBlock    blocks.Block

	// TODO: multiple manifests
	mainManifest message.LargeBlockManifest
	latestEntry  int
	entryOngoing bool
	done         bool
}

func NewLargeBlockSubSession(mcid cid.Cid) (*LargeBlockSubSession, error) {
	dec, err := multihash.Decode(mcid.Hash())
	if err != nil {
		return nil, err
	}
	bs := blocksplitter.GetBlockSplitter(dec.Code)

	verifier, err := bs.NewManifestVerifier(mcid.Hash())
	if err != nil {
		return nil, err
	}
	return &LargeBlockSubSession{
		manifests:      make(map[peer.ID]message.LargeBlockManifest),
		verifier:       verifier,
		latestEntry:    -1,
		mcid:           mcid,
		verifiedChunks: make([]*blocksplitter.VerifierEntry, 0),
	}, nil
}

func (s *LargeBlockSubSession) AddManifest(from peer.ID, manifest message.LargeBlockManifest) {
	s.sessMx.Lock()
	defer s.sessMx.Unlock()
	if _, ok := s.manifests[from]; !ok {
		if len(s.manifests) == 0 {
			s.mainManifest = manifest
		}
		s.manifests[from] = manifest
	} else {
		lbslog.Debugf("received duplicate manifest from %v", from)
	}
}

func (s *LargeBlockSubSession) Next() []cid.Cid {
	s.sessMx.Lock()
	defer s.sessMx.Unlock()

	if s.done {
		return nil
	}

	// TODO: Only handling one chunk at a time

	wantCids := make([]cid.Cid, 0)
	m := s.mainManifest.BlockManifest.Manifest
	if len(m) > s.latestEntry+1 {
		s.latestEntry++
		for _, c := range m[s.latestEntry].ChunkedBlocks {
			wantCids = append(wantCids, c.Cid.Cid)
		}
		s.entryOngoing = true
	} else {
		s.done = true

		sz := 0
		for _, e := range s.verifiedChunks {
			sz += len(e.Data)
		}

		blockData := make([]byte, sz)
		for _, e := range s.verifiedChunks {
			copy(blockData[e.StartIndex:], e.Data)
		}

		hash, err2 := multihash.Sum(blockData, multihash.SHA2_256, -1)
		if err2 != nil {
			panic(err2)
		}

		if !bytes.Equal(hash, s.mcid.Hash()) {
			lbslog.Errorf("hashes don't match")
		}

		var err error
		s.resultBlock, err = blocks.NewBlockWithCid(blockData, s.mcid)
		if err != nil {
			lbslog.Errorf("unable to construct block %s : %v", s.mcid, err)
			return nil
		}
	}

	return wantCids
}

func (s *LargeBlockSubSession) Done() bool {
	s.sessMx.Lock()
	defer s.sessMx.Unlock()

	return s.done
}

func (s *LargeBlockSubSession) AddBlock(block blocks.Block) {
	s.sessMx.Lock()
	defer s.sessMx.Unlock()

	// TODO: Only handling one chunk at a time
	e := s.mainManifest.BlockManifest.Manifest[s.latestEntry]

	// TODO: Assuming one block per chunk
	entries, verified, err := s.verifier.AddBytes(&blocksplitter.VerifierEntry{
		Data:       block.RawData(),
		Proof:      e.Proof,
		StartIndex: e.FullBlockStartIndex,
		EndIndex:   e.FullBlockEndIndex,
	})
	if err != nil {
		lbslog.Debugf("could not add bytes to verifier %v", err)
	}

	// TODO: This currently implies a bad manifest, since we're searching by CID so do something about it (e.g. change manifests)
	for i, v := range verified {
		if !v {
			lbslog.Errorf("block was not valid")
			continue
		}
		s.verifiedChunks = append(s.verifiedChunks, entries[i])
	}
}
