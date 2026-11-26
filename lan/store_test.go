package lan

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func TestStoreDeleteRangeKeepsItsBounds(t *testing.T) {
	s := newStore()
	for i := uint64(1); i <= 100; i++ {
		if err := s.StoreLog(&raft.Log{Index: i, Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(lo, hi uint64, n int) {
		t.Helper()
		first, _ := s.FirstIndex()
		last, _ := s.LastIndex()
		if first != lo || last != hi || len(s.logs) != n {
			t.Fatalf("holds %d..%d (%d entries), want %d..%d (%d)", first, last, len(s.logs), lo, hi, n)
		}
	}
	_ = s.DeleteRange(0, 90) // a prefix, from below the first entry
	check(91, 100, 10)
	var l raft.Log
	if s.GetLog(90, &l) != raft.ErrLogNotFound || s.GetLog(91, &l) != nil || l.Index != 91 {
		t.Fatal("entries around the cut", l)
	}
	_ = s.DeleteRange(1, 50) // nothing left there
	check(91, 100, 10)
	_ = s.DeleteRange(98, 200) // a suffix
	check(91, 97, 7)
	_ = s.DeleteRange(91, 97)
	check(0, 0, 0)
	_ = s.StoreLog(&raft.Log{Index: 500})
	check(500, 500, 1)
}

// A snapshot being written is not offered: the host would send half of it.
func TestSnapshotIsOfferedOnlyOnceWhole(t *testing.T) {
	s := &snapshots{}
	k, _ := s.Create(1, 10, 2, raft.Configuration{}, 1, nil)
	k.Write([]byte("hal"))
	if l, _ := s.List(); len(l) != 0 {
		t.Fatal("listed while written:", l)
	}
	k.Write([]byte("f"))
	k.Close()
	next, _ := s.Create(1, 20, 2, raft.Configuration{}, 1, nil)
	next.Write([]byte("partial"))
	l, _ := s.List()
	meta, r, err := s.Open(k.ID())
	if len(l) != 1 || l[0].ID != k.ID() || err != nil || meta.Size != 4 {
		t.Fatal(l, meta, err)
	}
	if b, _ := io.ReadAll(r); string(b) != "half" {
		t.Fatalf("%q", b)
	}
	if _, _, err := s.Open(next.ID()); err == nil {
		t.Fatal("opened an unfinished snapshot")
	}
	// A snapshot that finishes after a newer one must not replace it.
	late, _ := s.Create(1, 15, 2, raft.Configuration{}, 1, nil)
	next.Close()
	late.Write([]byte("older"))
	late.Close()
	if l, _ := s.List(); len(l) != 1 || l[0].Index != 20 {
		t.Fatal("an older snapshot replaced a newer one:", l)
	}
	if _, r, err := s.Open(next.ID()); err != nil {
		t.Fatal(err)
	} else if b, _ := io.ReadAll(r); string(b) != "partial" {
		t.Fatalf("%q", b)
	}
}
