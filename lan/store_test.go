package lan

import (
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
