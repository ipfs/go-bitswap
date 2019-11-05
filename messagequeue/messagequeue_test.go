package messagequeue

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/message"
	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"

	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakeMessageNetwork struct {
	connectError       error
	messageSenderError error
	messageSender      bsnet.MessageSender
}

func (fmn *fakeMessageNetwork) ConnectTo(context.Context, peer.ID) error {
	return fmn.connectError
}

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error) {
	if fmn.messageSenderError == nil {
		return fmn.messageSender, nil
	}
	return nil, fmn.messageSenderError
}

func (fms *fakeMessageNetwork) Self() peer.ID { return "" }

type fakeMessageSender struct {
	sendError    error
	fullClosed   chan<- struct{}
	reset        chan<- struct{}
	messagesSent chan<- bsmsg.BitSwapMessage
	supportsHave bool
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	fms.messagesSent <- msg
	return fms.sendError
}
func (fms *fakeMessageSender) Close() error       { fms.fullClosed <- struct{}{}; return nil }
func (fms *fakeMessageSender) Reset() error       { fms.reset <- struct{}{}; return nil }
func (fms *fakeMessageSender) SupportsHave() bool { return fms.supportsHave }

func collectMessages(ctx context.Context,
	t *testing.T,
	messagesSent <-chan bsmsg.BitSwapMessage,
	timeout time.Duration) []bsmsg.BitSwapMessage {
	var messagesReceived []bsmsg.BitSwapMessage
	timeoutctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case messageReceived := <-messagesSent:
			messagesReceived = append(messagesReceived, messageReceived)
		case <-timeoutctx.Done():
			return messagesReceived
		}
	}
}

func totalEntriesLength(messages []bsmsg.BitSwapMessage) int {
	totalLength := 0
	for _, messages := range messages {
		totalLength += len(messages.Wantlist())
	}
	return totalLength
}

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	bcstwh := testutil.GenerateCids(10)

	messageQueue.Startup()
	messageQueue.AddBroadcastWantHaves(bcstwh)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent for broadcast want-haves")
	}

	firstMessage := messages[0]
	if len(firstMessage.Wantlist()) != len(bcstwh) {
		t.Fatal("did not add all wants to want list")
	}
	for _, entry := range firstMessage.Wantlist() {
		if entry.Cancel {
			t.Fatal("initial add sent cancel entry when it should not have")
		}
	}

	messageQueue.Shutdown()

	timeoutctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	select {
	case <-fullClosedChan:
	case <-resetChan:
		t.Fatal("message sender should have been closed but was reset")
	case <-timeoutctx.Done():
		t.Fatal("message sender should have been closed but wasn't")
	}
}

func TestSendingMessagesDeduped(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, wantHaves)
	messageQueue.AddWants(wantBlocks, wantHaves)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("Messages were not deduped")
	}
}

func TestSendingMessagesPartialDupe(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks[:8], wantHaves[:8])
	messageQueue.AddWants(wantBlocks[3:], wantHaves[3:])
	messages := collectMessages(ctx, t, messagesSent, 20*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("messages were not correctly deduped")
	}
}

func TestSendingMessagesPriority(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	wantHaves1 := testutil.GenerateCids(5)
	wantHaves2 := testutil.GenerateCids(5)
	wantHaves := append(wantHaves1, wantHaves2...)
	wantBlocks1 := testutil.GenerateCids(5)
	wantBlocks2 := testutil.GenerateCids(5)
	wantBlocks := append(wantBlocks1, wantBlocks2...)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks1, wantHaves1)
	messageQueue.AddWants(wantBlocks2, wantHaves2)
	messages := collectMessages(ctx, t, messagesSent, 20*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}
	byCid := make(map[cid.Cid]message.Entry)
	for _, entry := range messages[0].Wantlist() {
		byCid[entry.Cid] = entry
	}

	// Check that earliest want-haves have highest priority
	for i := range wantHaves {
		if i > 0 {
			if byCid[wantHaves[i]].Priority > byCid[wantHaves[i-1]].Priority {
				t.Fatal("earliest want-haves should have higher priority")
			}
		}
	}

	// Check that earliest want-blocks have highest priority
	for i := range wantBlocks {
		if i > 0 {
			if byCid[wantBlocks[i]].Priority > byCid[wantBlocks[i-1]].Priority {
				t.Fatal("earliest want-blocks should have higher priority")
			}
		}
	}

	// Check that want-haves have higher priority than want-blocks within
	// same group
	for i := range wantHaves1 {
		if i > 0 {
			if byCid[wantHaves[i]].Priority <= byCid[wantBlocks[0]].Priority {
				t.Fatal("want-haves should have higher priority than want-blocks")
			}
		}
	}

	// Check that all items in first group have higher priority than first item
	// in second group
	for i := range wantHaves1 {
		if i > 0 {
			if byCid[wantHaves[i]].Priority <= byCid[wantHaves2[0]].Priority {
				t.Fatal("items in first group should have higher priority than items in second group")
			}
		}
	}
}

func TestWantlistRebroadcast(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	bcstwh := testutil.GenerateCids(10)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	// Add some broadcast want-haves
	messageQueue.Startup()
	messageQueue.AddBroadcastWantHaves(bcstwh)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent for initial wants")
	}

	// All broadcast want-haves should have been sent
	firstMessage := messages[0]
	if len(firstMessage.Wantlist()) != len(bcstwh) {
		t.Fatal("wrong number of wants")
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	// All the want-haves should have been rebroadcast
	firstMessage = messages[0]
	if len(firstMessage.Wantlist()) != len(bcstwh) {
		t.Fatal("did not rebroadcast all wants")
	}

	// Tell message queue to rebroadcast after a long time (so it doesn't
	// interfere with the next message collection), then send out some
	// regular wants and collect them
	messageQueue.SetRebroadcastInterval(1 * time.Second)
	messageQueue.AddWants(wantBlocks, wantHaves)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	// All new wants should have been sent
	firstMessage = messages[0]
	if len(firstMessage.Wantlist()) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	firstMessage = messages[0]

	// Both original and new wants should have been rebroadcast
	totalWants := len(bcstwh) + len(wantHaves) + len(wantBlocks)
	if len(firstMessage.Wantlist()) != totalWants {
		t.Fatal("did not rebroadcast all wants")
	}

	// Cancel some of the wants
	messageQueue.SetRebroadcastInterval(1 * time.Second)
	cancels := append([]cid.Cid{bcstwh[0]}, wantHaves[0], wantBlocks[0])
	messageQueue.AddCancels(cancels)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	// Cancels for each want should have been sent
	firstMessage = messages[0]
	if len(firstMessage.Wantlist()) != len(cancels) {
		t.Fatal("wrong number of cancels")
	}
	for _, entry := range firstMessage.Wantlist() {
		if !entry.Cancel {
			t.Fatal("expected cancels")
		}
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	firstMessage = messages[0]
	if len(firstMessage.Wantlist()) != totalWants-len(cancels) {
		t.Fatal("did not rebroadcast all wants")
	}
}

func TestSendingLargeMessages(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	wantBlocks := testutil.GenerateCids(10)
	entrySize := 44
	maxMsgSize := entrySize * 3 // 3 wants
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMsgSize)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, []cid.Cid{})
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// want-block has size 44, so with maxMsgSize 44 * 3 (3 want-blocks), then if
	// we send 10 want-blocks we should expect 4 messages:
	// [***] [***] [***] [*]
	if len(messages) != 4 {
		t.Fatal("expected 4 messages to be sent, got", len(messages))
	}
	if totalEntriesLength(messages) != len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}
}

func TestSendToPeerThatDoesntSupportHave(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, false}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	messageQueue := New(ctx, peerID, fakenet)
	messageQueue.Startup()

	// If the remote peer doesn't support HAVE / DONT_HAVE messages
	// - want-blocks should be sent normally
	// - want-haves should not be sent
	// - broadcast want-haves should be sent as want-blocks

	// Check broadcast want-haves
	bcwh := testutil.GenerateCids(10)
	messageQueue.AddBroadcastWantHaves(bcwh)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent", len(messages))
	}
	wl := messages[0].Wantlist()
	if len(wl) != len(bcwh) {
		t.Fatal("wrong number of entries in wantlist", len(wl))
	}
	for _, entry := range wl {
		if entry.WantType != pb.Message_Wantlist_Block {
			t.Fatal("broadcast want-haves should be sent as want-blocks")
		}
	}

	// Check regular want-haves and want-blocks
	wbs := testutil.GenerateCids(10)
	whs := testutil.GenerateCids(10)
	messageQueue.AddWants(wbs, whs)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent", len(messages))
	}
	wl = messages[0].Wantlist()
	if len(wl) != len(wbs) {
		t.Fatal("should only send want-blocks (no want-haves)", len(wl))
	}
	for _, entry := range wl {
		if entry.WantType != pb.Message_Wantlist_Block {
			t.Fatal("should only send want-blocks")
		}
	}
}
