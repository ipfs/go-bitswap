package message

import (
	"encoding/binary"
	"fmt"
	"io"

	pb "github.com/ipfs/go-bitswap/message/pb"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	blocks "github.com/ipfs/go-block-format"

	cid "github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	msgio "github.com/libp2p/go-msgio"

	"github.com/libp2p/go-libp2p-core/network"
)

// BitSwapMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the BitSwap protocol.
type BitSwapMessage interface {
	// Wantlist returns a slice of unique keys that represent data wanted by
	// the sender.
	Wantlist() []Entry

	// Blocks returns a slice of unique blocks.
	Blocks() []blocks.Block
	BlockInfos() []pb.Message_BlockInfo
	Haves() []cid.Cid
	DontHaves() []cid.Cid

	// AddEntry adds an entry to the Wantlist.
	AddEntry(key cid.Cid, priority int, wantType wantlist.WantTypeT, sendDontHave bool)

	Cancel(key cid.Cid)

	Empty() bool

	// A full wantlist is an authoritative copy, a 'non-full' wantlist is a patch-set
	Full() bool

	AddBlock(blocks.Block)
	AddBlockInfo(cid.Cid, pb.Message_BlockInfoType)
	AddHave(cid.Cid)
	AddDontHave(cid.Cid)
	Exportable

	Loggable() map[string]interface{}
}

// Exportable is an interface for structures than can be
// encoded in a bitswap protobuf.
type Exportable interface {
	ToProtoV0() *pb.Message
	ToProtoV1() *pb.Message
	ToNetV0(w io.Writer) error
	ToNetV1(w io.Writer) error
}

type impl struct {
	full       bool
	wantlist   map[cid.Cid]*Entry
	blocks     map[cid.Cid]blocks.Block
	blockInfos map[cid.Cid]pb.Message_BlockInfoType
}

// New returns a new, empty bitswap message
func New(full bool) BitSwapMessage {
	return newMsg(full)
}

func newMsg(full bool) *impl {
	return &impl{
		blocks:     make(map[cid.Cid]blocks.Block),
		blockInfos: make(map[cid.Cid]pb.Message_BlockInfoType),
		wantlist:   make(map[cid.Cid]*Entry),
		full:       full,
	}
}

// Entry is a wantlist entry in a Bitswap message, with flags indicating
// - whether message is a cancel
// - whether requester wants a DONT_HAVE message
// - whether requester wants a HAVE message (instead of the block)
type Entry struct {
	wantlist.Entry
	Cancel       bool
	SendDontHave bool
	WantType     wantlist.WantTypeT
}

func newMessageFromProto(pbm pb.Message) (BitSwapMessage, error) {
	m := newMsg(pbm.Wantlist.Full)
	for _, e := range pbm.Wantlist.Entries {
		c, err := cid.Cast([]byte(e.Block))
		if err != nil {
			return nil, fmt.Errorf("incorrectly formatted cid in wantlist: %s", err)
		}
		m.addEntry(c, int(e.Priority), e.Cancel, pb2go(e.WantType), e.SendDontHave)
	}

	// deprecated
	for _, d := range pbm.Blocks {
		// CIDv0, sha256, protobuf only
		b := blocks.NewBlock(d)
		m.AddBlock(b)
	}
	//

	for _, b := range pbm.GetPayload() {
		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return nil, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return nil, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return nil, err
		}

		m.AddBlock(blk)
	}

	for _, bi := range pbm.GetBlockInfos() {
		c, err := cid.Cast(bi.GetCid())
		if err != nil {
			return nil, err
		}

		t := bi.GetType()
		m.AddBlockInfo(c, t)
	}

	return m, nil
}

func (m *impl) Full() bool {
	return m.full
}

func (m *impl) Empty() bool {
	return len(m.blocks) == 0 && len(m.wantlist) == 0 && len(m.blockInfos) == 0
}

func (m *impl) Wantlist() []Entry {
	out := make([]Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		out = append(out, *e)
	}
	return out
}

func (m *impl) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(m.blocks))
	for _, block := range m.blocks {
		bs = append(bs, block)
	}
	return bs
}

func (m *impl) BlockInfos() []pb.Message_BlockInfo {
	bis := make([]pb.Message_BlockInfo, 0, len(m.blockInfos))
	for c, t := range m.blockInfos {
		bis = append(bis, pb.Message_BlockInfo{c.Bytes(), t})
	}
	return bis
}

func (m *impl) Haves() []cid.Cid {
	return m.getBlockInfoByType(pb.Message_Have)
}

func (m *impl) DontHaves() []cid.Cid {
	return m.getBlockInfoByType(pb.Message_DontHave)
}

func (m *impl) getBlockInfoByType(t pb.Message_BlockInfoType) []cid.Cid {
	cids := make([]cid.Cid, 0)
	for c, bit := range m.blockInfos {
		if bit == t {
			cids = append(cids, c)
		}
	}
	return cids
}

func (m *impl) Cancel(k cid.Cid) {
	m.addEntry(k, 0, true, false, false)
}

func (m *impl) AddEntry(k cid.Cid, priority int, wantType wantlist.WantTypeT, sendDontHave bool) {
	m.addEntry(k, priority, false, wantType, sendDontHave)
}

func (m *impl) addEntry(c cid.Cid, priority int, cancel bool, wantType wantlist.WantTypeT, sendDontHave bool) {
	e, exists := m.wantlist[c]
	if exists {
		e.Priority = priority
		e.Cancel = cancel
		e.SendDontHave = sendDontHave
		// Want for a block overrides existing want for a HAVE
		if wantType == wantlist.WantType_Block || e.WantType == wantlist.WantType_Have {
			e.WantType = wantType
		}
		m.wantlist[c] = e
	} else {
		m.wantlist[c] = &Entry{
			Entry: wantlist.Entry{
				Cid:      c,
				Priority: priority,
			},
			WantType:     wantType,
			SendDontHave: sendDontHave,
			Cancel:       cancel,
		}
	}
}

func (m *impl) AddBlock(b blocks.Block) {
	m.blocks[b.Cid()] = b
}

func (m *impl) AddBlockInfo(c cid.Cid, t pb.Message_BlockInfoType) {
	m.blockInfos[c] = t
}

func (m *impl) AddHave(c cid.Cid) {
	m.AddBlockInfo(c, pb.Message_Have)
}

func (m *impl) AddDontHave(c cid.Cid) {
	m.AddBlockInfo(c, pb.Message_DontHave)
}

// FromNet generates a new BitswapMessage from incoming data on an io.Reader.
func FromNet(r io.Reader) (BitSwapMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return FromMsgReader(reader)
}

// FromPBReader generates a new Bitswap message from a gogo-protobuf reader
func FromMsgReader(r msgio.Reader) (BitSwapMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return nil, err
	}

	var pb pb.Message
	err = pb.Unmarshal(msg)
	r.ReleaseMsg(msg)
	if err != nil {
		return nil, err
	}

	return newMessageFromProto(pb)
}

func (m *impl) ToProtoV0() *pb.Message {
	pbm := new(pb.Message)
	pbm.Wantlist.Entries = make([]pb.Message_Wantlist_Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		pbm.Wantlist.Entries = append(pbm.Wantlist.Entries, pb.Message_Wantlist_Entry{
			Block:        e.Cid.Bytes(),
			Priority:     int32(e.Priority),
			Cancel:       e.Cancel,
			WantType:     go2pb(e.WantType),
			SendDontHave: e.SendDontHave,
		})
	}
	pbm.Wantlist.Full = m.full

	blocks := m.Blocks()
	pbm.Blocks = make([][]byte, 0, len(blocks))
	for _, b := range blocks {
		pbm.Blocks = append(pbm.Blocks, b.RawData())
	}
	return pbm
}

func (m *impl) ToProtoV1() *pb.Message {
	pbm := new(pb.Message)
	pbm.Wantlist.Entries = make([]pb.Message_Wantlist_Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		pbm.Wantlist.Entries = append(pbm.Wantlist.Entries, pb.Message_Wantlist_Entry{
			Block:        e.Cid.Bytes(),
			Priority:     int32(e.Priority),
			Cancel:       e.Cancel,
			WantType:     go2pb(e.WantType),
			SendDontHave: e.SendDontHave,
		})
	}
	pbm.Wantlist.Full = m.full

	blocks := m.Blocks()
	pbm.Payload = make([]pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Payload = append(pbm.Payload, pb.Message_Block{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
		})
	}

	pbm.BlockInfos = make([]pb.Message_BlockInfo, 0, len(m.blockInfos))
	for c, t := range m.blockInfos {
		pbm.BlockInfos = append(pbm.BlockInfos, pb.Message_BlockInfo{c.Bytes(), t})
	}

	return pbm
}

func (m *impl) ToNetV0(w io.Writer) error {
	return write(w, m.ToProtoV0())
}

func (m *impl) ToNetV1(w io.Writer) error {
	return write(w, m.ToProtoV1())
}

func write(w io.Writer, m *pb.Message) error {
	size := m.Size()

	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	written, err := m.MarshalTo(buf[n:])
	if err != nil {
		return err
	}
	n += written

	_, err = w.Write(buf[:n])
	return err
}

func (m *impl) Loggable() map[string]interface{} {
	blocks := make([]string, 0, len(m.blocks))
	for _, v := range m.blocks {
		blocks = append(blocks, v.Cid().String())
	}
	return map[string]interface{}{
		"blocks": blocks,
		"wants":  m.Wantlist(),
	}
}

func pb2go(wantType pb.Message_Wantlist_WantType) wantlist.WantTypeT {
	wt := wantlist.WantType_Block
	if wantType == pb.Message_Wantlist_Have {
		wt = wantlist.WantType_Have
	}
	return wantlist.WantTypeT(wt)
}

func go2pb(wantType wantlist.WantTypeT) pb.Message_Wantlist_WantType {
	wt := pb.Message_Wantlist_Block
	if wantType == wantlist.WantType_Have {
		wt = pb.Message_Wantlist_Have
	}
	return wt
}
