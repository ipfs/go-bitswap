package blocksplitter

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"fmt"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
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
	msg.Cid = pb.Cid{Cid:block.Cid()}
	msg.BlockSize = int64(len(block.RawData()))
	endIndex := len(block.RawData())-1
	for n != nil {
		entry := &pb.Message_BlockManifest_BlockManifestEntry{
			Proof:               n.DataHashPrefix,
			FullBlockStartIndex: int64(endIndex - len(n.Data)),
			FullBlockEndIndex:   int64(endIndex),
			ChunkedBlocks:       nil,
		}
		msg.Manifest = append(msg.Manifest, entry)
	}


	return msg, nil
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
