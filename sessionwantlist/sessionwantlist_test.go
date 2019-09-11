package sessionwantlist

import (
	"os"
	"testing"

	"github.com/ipfs/go-bitswap/testutil"

	cid "github.com/ipfs/go-cid"
)

var c0 cid.Cid
var c1 cid.Cid

const s0 = uint64(0)
const s1 = uint64(1)

func setup() {
	cids := testutil.GenerateCids(2)
	c0 = cids[0]
	c1 = cids[1]
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

func TestEmpty(t *testing.T) {
	swl := NewSessionWantlist()

	if len(swl.Keys()) != 0 {
		t.Fatal("Expected Keys() to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 0 {
		t.Fatal("Expected SessionsFor() to be empty")
	}
}

func TestSimpleAdd(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0
	swl.Add([]cid.Cid{c0}, s0)
	if len(swl.Keys()) != 1 {
		t.Fatal("Expected Keys() to have length 1")
	}
	if !swl.Keys()[0].Equals(c0) {
		t.Fatal("Expected Keys() to be [cid0]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor() to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s0 {
		t.Fatal("Expected SessionsFor() to be [s0]")
	}

	// s0: c0, c1
	swl.Add([]cid.Cid{c1}, s0)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !testutil.MatchIgnoreOrder(swl.Keys(), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Keys() to contain [cid0, cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor() to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s0 {
		t.Fatal("Expected SessionsFor() to be [s0]")
	}

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0}, s1)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !testutil.MatchIgnoreOrder(swl.Keys(), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Keys() to contain [cid0, cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 2 {
		t.Fatal("Expected SessionsFor() to have length 2")
	}
}

func TestMultiKeyAdd(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	swl.Add([]cid.Cid{c0, c1}, s0)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !testutil.MatchIgnoreOrder(swl.Keys(), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Keys() to contain [cid0, cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor() to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s0 {
		t.Fatal("Expected SessionsFor() to be [s0]")
	}
}

func TestSimpleRemoveKeys(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0, c1}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// s0: c1
	swl.RemoveKeys([]cid.Cid{c0})
	if len(swl.Keys()) != 1 {
		t.Fatal("Expected Keys() to have length 1")
	}
	if !swl.Keys()[0].Equals(c1) {
		t.Fatal("Expected Keys() to be [cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 0 {
		t.Fatal("Expected SessionsFor(c0) to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c1})) != 1 {
		t.Fatal("Expected SessionsFor(c1) to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c1})[0] != s0 {
		t.Fatal("Expected SessionsFor(c1) to be [s0]")
	}
}

func TestMultiRemoveKeys(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0, c1}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// <empty>
	swl.RemoveKeys([]cid.Cid{c0, c1})
	if len(swl.Keys()) != 0 {
		t.Fatal("Expected Keys() to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 0 {
		t.Fatal("Expected SessionsFor() to be empty")
	}
}

func TestRemoveSession(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0, c1}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// s1: c0
	swl.RemoveSession(s0)
	if len(swl.Keys()) != 1 {
		t.Fatal("Expected Keys() to have length 1")
	}
	if !swl.Keys()[0].Equals(c0) {
		t.Fatal("Expected Keys() to be [cid0]")
	}
	if len(swl.SessionsFor([]cid.Cid{c1})) != 0 {
		t.Fatal("Expected SessionsFor(c1) to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor(c0) to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s1 {
		t.Fatal("Expected SessionsFor(c0) to be [s1]")
	}
}
