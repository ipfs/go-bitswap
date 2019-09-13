package decision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	message "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	lu "github.com/ipfs/go-bitswap/logutil"

	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	testutil "github.com/libp2p/go-libp2p-core/test"
)

type fakePeerTagger struct {
	lk          sync.Mutex
	wait        sync.WaitGroup
	taggedPeers []peer.ID
}

func (fpt *fakePeerTagger) TagPeer(p peer.ID, tag string, n int) {
	fpt.wait.Add(1)

	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	fpt.taggedPeers = append(fpt.taggedPeers, p)
}

func (fpt *fakePeerTagger) UntagPeer(p peer.ID, tag string) {
	defer fpt.wait.Done()

	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	for i := 0; i < len(fpt.taggedPeers); i++ {
		if fpt.taggedPeers[i] == p {
			fpt.taggedPeers[i] = fpt.taggedPeers[len(fpt.taggedPeers)-1]
			fpt.taggedPeers = fpt.taggedPeers[:len(fpt.taggedPeers)-1]
			return
		}
	}
}

func (fpt *fakePeerTagger) count() int {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	return len(fpt.taggedPeers)
}

type engineSet struct {
	PeerTagger *fakePeerTagger
	Peer       peer.ID
	Engine     *Engine
	Blockstore blockstore.Blockstore
}

func newEngine(ctx context.Context, idStr string) engineSet {
	fpt := &fakePeerTagger{}
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	return engineSet{
		Peer: peer.ID(idStr),
		//Strategy: New(true),
		PeerTagger: fpt,
		Blockstore: bs,
		Engine: NewEngine(ctx, bs, fpt, "", 0),
	}
}

func TestConsistentAccounting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := newEngine(ctx, "Ernie")
	receiver := newEngine(ctx, "Bert")

	// Send messages from Ernie to Bert
	for i := 0; i < 1000; i++ {

		m := message.New(false)
		content := []string{"this", "is", "message", "i"}
		m.AddBlock(blocks.NewBlock([]byte(strings.Join(content, " "))))

		sender.Engine.MessageSent(receiver.Peer, m)
		receiver.Engine.MessageReceived(sender.Peer, m)
	}

	// Ensure sender records the change
	if sender.Engine.numBytesSentTo(receiver.Peer) == 0 {
		t.Fatal("Sent bytes were not recorded")
	}

	// Ensure sender and receiver have the same values
	if sender.Engine.numBytesSentTo(receiver.Peer) != receiver.Engine.numBytesReceivedFrom(sender.Peer) {
		t.Fatal("Inconsistent book-keeping. Strategies don't agree")
	}

	// Ensure sender didn't record receving anything. And that the receiver
	// didn't record sending anything
	if receiver.Engine.numBytesSentTo(sender.Peer) != 0 || sender.Engine.numBytesReceivedFrom(receiver.Peer) != 0 {
		t.Fatal("Bert didn't send bytes to Ernie")
	}
}

func TestPeerIsAddedToPeersWhenMessageReceivedOrSent(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sanfrancisco := newEngine(ctx, "sf")
	seattle := newEngine(ctx, "sea")

	m := message.New(true)

	sanfrancisco.Engine.MessageSent(seattle.Peer, m)
	seattle.Engine.MessageReceived(sanfrancisco.Peer, m)

	if seattle.Peer == sanfrancisco.Peer {
		t.Fatal("Sanity Check: Peers have same Key!")
	}

	if !peerIsPartner(seattle.Peer, sanfrancisco.Engine) {
		t.Fatal("Peer wasn't added as a Partner")
	}

	if !peerIsPartner(sanfrancisco.Peer, seattle.Engine) {
		t.Fatal("Peer wasn't added as a Partner")
	}

	seattle.Engine.PeerDisconnected(sanfrancisco.Peer)
	if peerIsPartner(sanfrancisco.Peer, seattle.Engine) {
		t.Fatal("expected peer to be removed")
	}
}

func peerIsPartner(p peer.ID, e *Engine) bool {
	for _, partner := range e.Peers() {
		if partner == p {
			return true
		}
	}
	return false
}

func TestOutboxClosedWhenEngineClosed(t *testing.T) {
	t.SkipNow() // TODO implement *Engine.Close
	e := NewEngine(context.Background(), blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())), &fakePeerTagger{}, "", 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for nextEnvelope := range e.Outbox() {
			<-nextEnvelope
		}
		wg.Done()
	}()
	// e.Close()
	wg.Wait()
	if _, ok := <-e.Outbox(); ok {
		t.Fatal("channel should be closed")
	}
}

func TestPartnerWantHaveWantBlock(t *testing.T) {
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	vowels := "aeiou"

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	for _, letter := range strings.Split(alphabet, "") {
		block := blocks.NewBlock([]byte(letter))
		if err := bs.Put(block); err != nil {
			t.Fatal(err)
		}
	}

	partner := testutil.RandPeerIDFatal(t)
	// partnerWantBlocks(e, vowels, partner)

	type testCaseEntry struct {
		wantBlks string
		wantHaves string
		sendDontHave bool
	}

	type testCaseExp struct {
		blks string
		haves string
		dontHaves string
	}

	type testCase struct {
		only bool
		wls []testCaseEntry
		exp []testCaseExp
	}

	testCases := []testCase{
		// Just send want-blocks
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: vowels,
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: vowels,
				},
			},
		},

		// Send want-blocks and want-haves
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: vowels,
					wantHaves: "fgh",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: vowels,
					haves: "fgh",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-haves that are not
		// present, but without requesting DONT_HAVES
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: vowels,
					wantHaves: "fgh123",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: vowels,
					haves: "fgh",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-haves that are not
		// present, and request DONT_HAVES
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: vowels,
					wantHaves: "fgh123",
					sendDontHave: true,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: vowels,
					haves: "fgh",
					dontHaves: "123",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-blocks and want-haves that are not
		// present, but without requesting DONT_HAVES
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "aeiou123",
					wantHaves: "fgh456",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: "aeiou",
					haves: "fgh",
					dontHaves: "",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-blocks and want-haves that are not
		// present, and request DONT_HAVES
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "aeiou123",
					wantHaves: "fgh456",
					sendDontHave: true,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: "aeiou",
					haves: "fgh",
					dontHaves: "123456",
				},
			},
		},

		// Send repeated want-blocks
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "ae",
					sendDontHave: false,
				},
				testCaseEntry{
					wantBlks: "io",
					sendDontHave: false,
				},
				testCaseEntry{
					wantBlks: "u",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: "ae",
				},
				testCaseExp{
					blks: "io",
				},
				testCaseExp{
					blks: "u",
				},
			},
		},

		// Send repeated want-blocks and want-haves
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "ae",
					wantHaves: "jk",
					sendDontHave: false,
				},
				testCaseEntry{
					wantBlks: "io",
					wantHaves: "lm",
					sendDontHave: false,
				},
				testCaseEntry{
					wantBlks: "u",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: "ae",
					haves: "jk",
				},
				testCaseExp{
					blks: "io",
					haves: "lm",
				},
				testCaseExp{
					blks: "u",
				},
			},
		},

		// Send repeated want-blocks and want-haves, with some want-blocks and want-haves that are not
		// present, and request DONT_HAVES
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "ae12",
					wantHaves: "jk5",
					sendDontHave: true,
				},
				testCaseEntry{
					wantBlks: "io34",
					wantHaves: "lm",
					sendDontHave: true,
				},
				testCaseEntry{
					wantBlks: "u",
					wantHaves: "6",
					sendDontHave: true,
				},
			},
			exp: []testCaseExp{
				testCaseExp{
					blks: "ae",
					haves: "jk",
					dontHaves: "125",
				},
				testCaseExp{
					blks: "io",
					haves: "lm",
					dontHaves: "34",
				},
				testCaseExp{
					blks: "u",
					dontHaves: "6",
				},
			},
		},

		// Send want-block then want-have for same CID
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "a",
					sendDontHave: true,
				},
				testCaseEntry{
					wantHaves: "a",
					sendDontHave: true,
				},
			},
			// want-have should be ignored because there was already a
			// want-block for the same CID in the queue
			exp: []testCaseExp{
				testCaseExp{
					blks: "a",
				},
			},
		},

		// Send want-have then want-block for same CID
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantHaves: "b",
					sendDontHave: true,
				},
				testCaseEntry{
					wantBlks: "b",
					sendDontHave: true,
				},
			},
			// want-block should overwrite existing want-have
			exp: []testCaseExp{
				testCaseExp{
					blks: "b",
				},
			},
		},

		// Send want-block then want-block for same CID
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantBlks: "a",
					sendDontHave: true,
				},
				testCaseEntry{
					wantBlks: "a",
					sendDontHave: true,
				},
			},
			// second want-block should be ignored
			exp: []testCaseExp{
				testCaseExp{
					blks: "a",
				},
			},
		},

		// Send want-have then want-have for same CID
		testCase{
			wls: []testCaseEntry{
				testCaseEntry{
					wantHaves: "a",
					sendDontHave: true,
				},
				testCaseEntry{
					wantHaves: "a",
					sendDontHave: true,
				},
			},
			// second want-have should be ignored
			exp: []testCaseExp{
				testCaseExp{
					haves: "a",
				},
			},
		},
	}

	var onlyTestCases []testCase
	for _, testCase := range testCases {
		if testCase.only {
			onlyTestCases = append(onlyTestCases, testCase)
		}
	}
	if len(onlyTestCases) > 0 {
		testCases = onlyTestCases
	}

	e := NewEngine(context.Background(), bs, &fakePeerTagger{}, "", 0)
	for i, testCase := range testCases {
		t.Logf("Test case %d:", i)
		for _, wl := range testCase.wls {
			t.Logf("  want-blocks '%s' / want-haves '%s' / sendDontHave %t",
				wl.wantBlks, wl.wantHaves, wl.sendDontHave)
			wantBlks := strings.Split(wl.wantBlks, "")
			wantHaves := strings.Split(wl.wantHaves, "")
			partnerWantBlocksHaves(e, wantBlks, wantHaves, wl.sendDontHave, partner)
		}

		for _, exp := range testCase.exp {
			expBlks := strings.Split(exp.blks, "")
			expHaves := strings.Split(exp.haves, "")
			expDontHaves := strings.Split(exp.dontHaves, "")
			env, err := checkOutput(t, e, expBlks, expHaves, expDontHaves)
			if err != nil {
				t.Fatal(err)
			}
			env.Sent()
		}
	}
}

func checkOutput(t *testing.T, e *Engine, expBlks []string, expHaves []string, expDontHaves []string) (*Envelope, error) {
	next := <-e.Outbox()
	envelope := <-next

	blks := envelope.Message.Blocks()
	presences := envelope.Message.BlockPresences()

	// Verify payload message length
	if len(blks) != len(expBlks) {
		blkDiff := formatBlocksDiff(blks, expBlks)
		msg := fmt.Sprintf("Received %d blocks. Expected %d blocks:\n%s", len(blks), len(expBlks), blkDiff)
		return nil, errors.New(msg)
	}

	// Verify block presences message length
	expPresencesCount := len(expHaves) + len(expDontHaves)
	if len(presences) != expPresencesCount {
		presenceDiff := formatPresencesDiff(presences, expHaves, expDontHaves)
		return nil, errors.New(fmt.Sprintf("Received %d BlockPresences. Expected %d BlockPresences:\n%s",
			len(presences), expPresencesCount, presenceDiff))
	}

	// Verify payload message contents
	for _, k := range expBlks {
		found := false
		expected := blocks.NewBlock([]byte(k))
		for _, block := range blks {
			if block.Cid().Equals(expected.Cid()) {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New(formatBlocksDiff(blks, expBlks))
		}
	}

	// Verify HAVEs
	if err := checkPresence(presences, expHaves, pb.Message_Have); err != nil {
		return nil, errors.New(formatPresencesDiff(presences, expHaves, expDontHaves))
	}

	// Verify DONT_HAVEs
	if err := checkPresence(presences, expDontHaves, pb.Message_DontHave); err != nil {
		return nil, errors.New(formatPresencesDiff(presences, expHaves, expDontHaves))
	}

	return envelope, nil
}

func checkPresence(presences []pb.Message_BlockPresence, expPresence []string, presenceType pb.Message_BlockPresenceType) error {
	for _, k := range expPresence {
		found := false
		expected := blocks.NewBlock([]byte(k))
		for _, p := range presences {
			c, err := cid.Cast(p.GetCid())
			if err != nil {
				panic("could not parse cid")
			}
			if c.Equals(expected.Cid()) {
				found = true
				if p.GetType() != presenceType {
					return errors.New("type mismatch")
				}
				break
			}
		}
		if !found {
			return errors.New("not found")
		}
	}
	return nil
}

func formatBlocksDiff(blks []blocks.Block, expBlks []string) string {
	var out bytes.Buffer
	out.WriteString(fmt.Sprintf("Blocks (%d):\n", len(blks)))
	for _, b := range blks {
		out.WriteString(fmt.Sprintf("  %s: %s\n", lu.C(b.Cid()), b.RawData()))
	}
	out.WriteString(fmt.Sprintf("Expected (%d):\n", len(expBlks)))
	for _, k := range expBlks {
		expected := blocks.NewBlock([]byte(k))
		out.WriteString(fmt.Sprintf("  %s: %s\n", lu.C(expected.Cid()), k))
	}
	return out.String()
}

func formatPresencesDiff(presences []pb.Message_BlockPresence, expHaves []string, expDontHaves []string) string {
	var out bytes.Buffer
	out.WriteString(fmt.Sprintf("BlockPresences (%d):\n", len(presences)))
	for _, b := range presences {
		c, err := cid.Cast(b.GetCid())
		if err != nil {
			panic(err)
		}
		t := "HAVE"
		if b.GetType() == pb.Message_DontHave {
			t = "DONT_HAVE"
		}
		out.WriteString(fmt.Sprintf("  %s - %s\n", lu.C(c), t))
	}
	out.WriteString(fmt.Sprintf("Expected (%d):\n", len(expHaves) + len(expDontHaves)))
	for _, k := range expHaves {
		expected := blocks.NewBlock([]byte(k))
		out.WriteString(fmt.Sprintf("  %s: %s - HAVE\n", lu.C(expected.Cid()), k))
	}
	for _, k := range expDontHaves {
		expected := blocks.NewBlock([]byte(k))
		out.WriteString(fmt.Sprintf("  %s: %s - DONT_HAVE\n", lu.C(expected.Cid()), k))
	}
	return out.String()
}

func TestPartnerWantsThenCancels(t *testing.T) {
	numRounds := 10
	if testing.Short() {
		numRounds = 1
	}
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")

	type testCase [][]string
	testcases := []testCase{
		{
			alphabet, vowels,
		},
		{
			alphabet, stringsComplement(alphabet, vowels),
			alphabet[1:25], stringsComplement(alphabet[1:25], vowels), alphabet[2:25], stringsComplement(alphabet[2:25], vowels),
			alphabet[3:25], stringsComplement(alphabet[3:25], vowels), alphabet[4:25], stringsComplement(alphabet[4:25], vowels),
			alphabet[5:25], stringsComplement(alphabet[5:25], vowels), alphabet[6:25], stringsComplement(alphabet[6:25], vowels),
			alphabet[7:25], stringsComplement(alphabet[7:25], vowels), alphabet[8:25], stringsComplement(alphabet[8:25], vowels),
			alphabet[9:25], stringsComplement(alphabet[9:25], vowels), alphabet[10:25], stringsComplement(alphabet[10:25], vowels),
			alphabet[11:25], stringsComplement(alphabet[11:25], vowels), alphabet[12:25], stringsComplement(alphabet[12:25], vowels),
			alphabet[13:25], stringsComplement(alphabet[13:25], vowels), alphabet[14:25], stringsComplement(alphabet[14:25], vowels),
			alphabet[15:25], stringsComplement(alphabet[15:25], vowels), alphabet[16:25], stringsComplement(alphabet[16:25], vowels),
			alphabet[17:25], stringsComplement(alphabet[17:25], vowels), alphabet[18:25], stringsComplement(alphabet[18:25], vowels),
			alphabet[19:25], stringsComplement(alphabet[19:25], vowels), alphabet[20:25], stringsComplement(alphabet[20:25], vowels),
			alphabet[21:25], stringsComplement(alphabet[21:25], vowels), alphabet[22:25], stringsComplement(alphabet[22:25], vowels),
			alphabet[23:25], stringsComplement(alphabet[23:25], vowels), alphabet[24:25], stringsComplement(alphabet[24:25], vowels),
			alphabet[25:25], stringsComplement(alphabet[25:25], vowels),
		},
	}

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	for _, letter := range alphabet {
		block := blocks.NewBlock([]byte(letter))
		if err := bs.Put(block); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < numRounds; i++ {
		expected := make([][]string, 0, len(testcases))
		e := NewEngine(context.Background(), bs, &fakePeerTagger{}, "", 0)
		for _, testcase := range testcases {
			set := testcase[0]
			cancels := testcase[1]
			keeps := stringsComplement(set, cancels)
			expected = append(expected, keeps)

			partner := testutil.RandPeerIDFatal(t)

			partnerWantBlocks(e, set, partner)
			partnerCancels(e, cancels, partner)
		}
		if err := checkHandledInOrder(t, e, expected); err != nil {
			t.Logf("run #%d of %d", i, numRounds)
			t.Fatal(err)
		}
	}
}

func TestTaggingPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	sanfrancisco := newEngine(ctx, "sf")
	seattle := newEngine(ctx, "sea")

	keys := []string{"a", "b", "c", "d", "e"}
	for _, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		if err := sanfrancisco.Blockstore.Put(block); err != nil {
			t.Fatal(err)
		}
	}
	partnerWantBlocks(sanfrancisco.Engine, keys, seattle.Peer)
	next := <-sanfrancisco.Engine.Outbox()
	envelope := <-next

	if sanfrancisco.PeerTagger.count() != 1 {
		t.Fatal("Incorrect number of peers tagged")
	}
	envelope.Sent()
	<-sanfrancisco.Engine.Outbox()
	sanfrancisco.PeerTagger.wait.Wait()
	if sanfrancisco.PeerTagger.count() != 0 {
		t.Fatal("Peers should be untagged but weren't")
	}
}

func partnerWantBlocks(e *Engine, keys []string, partner peer.ID) {
	add := message.New(false)
	for i, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), len(keys)-i, pb.Message_Wantlist_Block, true)
	}
	e.MessageReceived(partner, add)
}

func partnerWantBlocksHaves(e *Engine, keys []string, wantHaves []string, sendDontHave bool, partner peer.ID) {
	add := message.New(false)
	priority := len(wantHaves) + len(keys)
	for _, letter := range wantHaves {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), priority, pb.Message_Wantlist_Have, sendDontHave)
		priority--
	}
	for _, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), priority, pb.Message_Wantlist_Block, sendDontHave)
		priority--
	}
	e.MessageReceived(partner, add)
}

func partnerCancels(e *Engine, keys []string, partner peer.ID) {
	cancels := message.New(false)
	for _, k := range keys {
		block := blocks.NewBlock([]byte(k))
		cancels.Cancel(block.Cid())
	}
	e.MessageReceived(partner, cancels)
}

func checkHandledInOrder(t *testing.T, e *Engine, expected [][]string) error {
	for _, keys := range expected {
		next := <-e.Outbox()
		envelope := <-next
		received := envelope.Message.Blocks()
		// Verify payload message length
		if len(received) != len(keys) {
			return errors.New(fmt.Sprintln("# blocks received", len(received), "# blocks expected", len(keys)))
		}
		// Verify payload message contents
		for _, k := range keys {
			found := false
			expected := blocks.NewBlock([]byte(k))
			for _, block := range received {
				if block.Cid().Equals(expected.Cid()) {
					found = true
					break
				}
			}
			if !found {
				return errors.New(fmt.Sprintln("received", received, "expected", string(expected.RawData())))
			}
		}
	}
	return nil
}

func stringsComplement(set, subset []string) []string {
	m := make(map[string]struct{})
	for _, letter := range subset {
		m[letter] = struct{}{}
	}
	var complement []string
	for _, letter := range set {
		if _, exists := m[letter]; !exists {
			complement = append(complement, letter)
		}
	}
	return complement
}
