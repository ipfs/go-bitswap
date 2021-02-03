package blocksplitter

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"fmt"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"math/rand"
)

const (
	sha2_256_chunk = 64
)

var (
	chunksize = int64(1024 * 1024) // 1MB for testing
)

type fixedSha256Splitter struct {
	chunkSize int
}

func Sha256Splitter(chunkSize int) *fixedSha256Splitter {
	numChunks := chunksize / sha2_256_chunk
	if numChunks*sha2_256_chunk != chunksize {
		panic(fmt.Errorf("chunk size must be a multiple of the sha256 chunksize which is %d", sha2_256_chunk))
	}
	return &fixedSha256Splitter{chunkSize: chunkSize}
}

func (s *fixedSha256Splitter) GetManifest(block blocks.Block) (*pb.Message_BlockManifest, error) {
	h := block.Cid().Hash()
	dec, err := multihash.Decode(h)
	if err != nil {
		return nil, err
	}
	if dec.Code != multihash.SHA2_256 {
		return nil, fmt.Errorf("unsupported hash code: only SHA2_256 is supported instead received code %d with name %s", dec.Code, dec.Name)
	}

	n := createTree(block.RawData(), s.chunkSize)
	msg := new(pb.Message_BlockManifest)
	msg.Cid = pb.Cid{Cid: block.Cid()}
	msg.BlockSize = int64(len(block.RawData()))
	endIndex := int64(len(block.RawData()) - 1)

	dec, err = multihash.Decode(block.Cid().Hash())
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(ShaPFinalize(ShaPCont(n.DataHashPrefix, n.Data)), dec.Digest) {
		panic("noo")
	}
	//// TODO: Should this be here, or should size be passed in specially?
	//sz := make([]byte, 8)
	//_ = binary.PutUvarint(sz, uint64(msg.BlockSize))
	//szMH, err := multihash.Sum(sz, multihash.IDENTITY, -1)
	//if err != nil {
	//	return nil, err
	//}
	//sizeEntry := &pb.Message_BlockManifest_BlockManifestEntry{
	//	Proof:               n.DataHashPrefix,
	//	FullBlockStartIndex: 0,
	//	FullBlockEndIndex:   -1,
	//	ChunkedBlocks: []*pb.Message_BlockManifest_BlockManifestEntry_ChunkedBlockManifestEntry{
	//		{
	//			Cid:             pb.Cid{Cid: cid.NewCidV1(cid.Raw, szMH)},
	//			BlockStartIndex: 0,
	//			BlockEndIndex:   int32(len(n.Data)),
	//		},
	//	},
	//}
	//msg.Manifest = append(msg.Manifest, sizeEntry)

	first := true

	for n != nil {
		entryHash, err := multihash.Sum(n.Data, multihash.SHA2_256, -1)
		if err != nil {
			return nil, err
		}
		entry := &pb.Message_BlockManifest_BlockManifestEntry{
			Proof:               n.DataHashPrefix,
			FullBlockStartIndex: endIndex - int64(len(n.Data)),
			FullBlockEndIndex:   endIndex,
			ChunkedBlocks: []*pb.Message_BlockManifest_BlockManifestEntry_ChunkedBlockManifestEntry{
				{
					Cid:             pb.Cid{Cid: cid.NewCidV1(cid.Raw, entryHash)},
					BlockStartIndex: 0,
					BlockEndIndex:   int32(len(n.Data)),
				},
			},
		}

		endIndex = entry.FullBlockStartIndex

		if first {
			entry.FullBlockEndIndex = -1
			first = false
		}

		msg.Manifest = append(msg.Manifest, entry)
		n = n.FilePrevNode
	}

	return msg, nil
}

func (s *fixedSha256Splitter) NewManifestVerifier(mh multihash.Multihash) (ManifestVerifier, error) {
	dec, err := multihash.Decode(mh)
	if err != nil {
		return nil, err
	}
	if dec.Code != multihash.SHA2_256 {
		return nil, fmt.Errorf("unsupported hash code: only SHA2_256 is supported instead received code %d with name %s", dec.Code, dec.Name)
	}

	return &fixedSha256SplitterVerifier{
		chunkSize: s.chunkSize,
		sid:       dec.Digest,
		latestIV:  dec.Digest,
	}, nil
}

type fixedSha256SplitterVerifier struct {
	chunkSize     int
	sid           []byte
	latestIV      []byte
	latestIVIndex int64

	queuedBlocks []*VerifierEntry
}

type VerifierEntry struct {
	Data                 []byte
	Proof                []byte
	StartIndex, EndIndex int64
}

// AddBytes takes in some data from a byte stream along with where it lives in the byte stream and a proof it belongs
// returns
func (v *fixedSha256SplitterVerifier) AddBytes(entry *VerifierEntry) ([]*VerifierEntry, []bool, error) {
	valid, unknown, err := v.internalAddBytes(entry)
	if err != nil {
		return nil, nil, err
	}

	if unknown {
		v.queuedBlocks = append(v.queuedBlocks, entry)
	}

	if !unknown && !valid {
		return []*VerifierEntry{entry}, []bool{false}, nil
	}

	// entry was valid
	entries := []*VerifierEntry{entry}
	validities := []bool{true}

	repeat := true
	for repeat {
		repeat = false
		for i, e := range v.queuedBlocks {
			valid, unknown, err = v.internalAddBytes(e)
			if err != nil {
				return nil, nil, err
			}

			if !unknown {
				// remove the element from the queue
				v.queuedBlocks[i] = v.queuedBlocks[len(v.queuedBlocks)-1]
				v.queuedBlocks = v.queuedBlocks[:len(v.queuedBlocks)-1]

				// we have information about a new entry to return
				entries = append(entries, e)
			}

			if valid {
				// mark entry as valid
				validities = append(validities, true)

				// see if we can process anything else now
				repeat = true
				break
			}

			// mark entry as invalid
			validities = append(validities, false)
		}
	}

	return entries, validities, nil
}

func (v *fixedSha256SplitterVerifier) internalAddBytes(entry *VerifierEntry) (valid bool, unknown bool, err error) {
	data := entry.Data
	proof := entry.Proof
	endIndex := entry.EndIndex

	// If it's the size byte
	if endIndex == -1 {
		_, sizeFromProof := consumeUint64(proof[len(proof)-8:])
		dataSz, _ := binary.Uvarint(data)
		if dataSz != sizeFromProof {
			//return false, false, nil
		}
		end := ShaPFinalize(ShaPCont(proof, data))
		if bytes.Compare(v.latestIV, end) != 0 {
			return false, false, nil
		}
		v.latestIV = proof
		v.latestIVIndex = int64(sizeFromProof) - 1
		return true, false, nil
	}

	// if it's the next verifiable piece of data
	if endIndex == v.latestIVIndex {
		calculated := ShaPCont(proof, data)
		if bytes.Compare(v.latestIV, calculated) != 0 {
			return false, false, nil
		}
		v.latestIV = proof
		v.latestIVIndex -= int64(len(data))
		return true, false, nil
	}

	// if we've already verified this data
	// Note: assumes the caller of AddBytes only passes the same data in once/handles duplicates
	if endIndex > v.latestIVIndex {
		return false, false, nil
	}

	// if it's too early for us to verify the data
	return false, true, nil
}

func main() {
	rng := rand.New(rand.NewSource(rand.Int63()))

	numChunks := chunksize / sha2_256_chunk
	if numChunks*sha2_256_chunk != chunksize {
		panic("only tested with full chunk sizes")
	}

	data := make([]byte, sha2_256_chunk*numChunks)
	_, err := rng.Read(data)
	if err != nil {
		panic(err)
	}

	h := sha256.New()
	h.Write(data)
	fileHash := h.Sum(nil)

	n := createTree(data, sha2_256_chunk)
	retData, ok := validateTree(n, fileHash)
	if !ok {
		panic("we failed")
	}

	if !bytes.Equal(data, retData) {
		panic("bad validation code")
	}

	fmt.Println("Success!")
}

type Node struct {
	FilePrevNode   *Node // Next node in the chain, moving towards earlier file data
	Data           []byte
	DataHashPrefix ShaPBytes // i.e. SHA256-P(F0 || ... Fi-1) if the data is Fi
}

func createTree(b []byte, chunk int) *Node {
	numChunks := len(b) / chunk
	if numChunks*chunk != len(b) {
		panic("invalid length")
	}

	// build the tree from furthest leaves up to the root
	var next *Node
	var nextHash ShaPBytes
	for i := 0; i < numChunks; i++ {
		chunkBytes := b[i*chunk : (i+1)*chunk]
		next = &Node{
			FilePrevNode:   next,
			Data:           chunkBytes,
			DataHashPrefix: nextHash,
		}
		nextHash = ShaPCont(nextHash, chunkBytes)
	}
	return next
}

func validateTree(nd *Node, fileHash []byte) ([]byte, bool) {
	// special root node check to deal with finalizing the hash
	dataBytes := nd.Data
	if !bytes.Equal(ShaPFinalize(ShaPCont(nd.DataHashPrefix, dataBytes)), fileHash) {
		return nil, false
	}

	var fullDataReverse [][]byte
	fullDataReverse = append(fullDataReverse, dataBytes)

	hash := nd.DataHashPrefix
	link := nd.FilePrevNode
	for link != nil {
		dataBytes = link.Data
		// check we are continuing the hash properly
		if !bytes.Equal(ShaPCont(link.DataHashPrefix, dataBytes), hash) {
			return nil, false
		}
		fullDataReverse = append(fullDataReverse, dataBytes)
		hash = link.DataHashPrefix
		link = link.FilePrevNode
	}

	// reassemble data fragments
	var fullData []byte
	for i := len(fullDataReverse) - 1; i >= 0; i-- {
		fullData = append(fullData, fullDataReverse[i]...)
	}

	return fullData, true
}

type ShaPBytes []byte

func ShaPCont(start ShaPBytes, b []byte) ShaPBytes {
	h := sha256.New()
	if start != nil {
		err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(start)
		if err != nil {
			panic(err)
		}
	}

	h.Write(b)
	hBytes, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}
	return hBytes
}

func ShaPFinalize(partial ShaPBytes) []byte {
	h := sha256.New()
	if partial != nil {
		err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(partial)
		if err != nil {
			panic(err)
		}
	}

	return h.Sum(nil)
}

func consumeUint64(b []byte) ([]byte, uint64) {
	_ = b[7]
	x := uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
	return b[8:], x
}
