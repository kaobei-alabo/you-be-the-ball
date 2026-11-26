package lan

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// compactOften makes raft snapshot every few dozen moves.
func compactOften(t *testing.T) {
	old := compaction
	compaction.interval, compaction.threshold, compaction.trailing = 20*time.Millisecond, 16, 8
	t.Cleanup(func() { compaction = old })
}

// span is the range of entries p's log holds; 0, 0 when empty.
func span(p *Player) (lo, hi uint64, n int) {
	p.logs.mu.Lock()
	defer p.logs.mu.Unlock()
	return p.logs.lo, p.logs.hi, len(p.logs.logs)
}

func stateOf(p *Player) *room.State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// moves makes n moves, round robin over ps, with IDs prefix0, prefix1...
func moves(t *testing.T, ps []*Player, prefix string, n int) {
	t.Helper()
	for i := range n {
		inc(t, ps[i%len(ps)], prefix+strconv.Itoa(i))
	}
}

// records waits for every player's record and checks they are identical.
func records(t *testing.T, ps []*Player, result string) {
	t.Helper()
	waitFor(t, "every record", func() bool {
		for _, p := range ps {
			if p.Status().Record == "" {
				return false
			}
		}
		return true
	})
	var first room.Record
	for i, p := range ps {
		r, err := room.ReadRecord(p.Status().Record)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = r
		} else if !reflect.DeepEqual(r, first) {
			t.Fatal("records differ:", r, first)
		}
	}
	if first.Status != "finished" || string(first.Result) != result {
		t.Fatal("record", first.Status, string(first.Result))
	}
}

func TestCompactionBringsAnOfflinePlayerBackFromTheSnapshot(t *testing.T) {
	compactOften(t)
	ps := play(t, room.Config{Players: 3}, fast)
	h := leader(t, ps...)
	off := without(ps, h)[0]
	on := without(ps, off)
	seq := inc(t, off, "mine")
	converged(t, ps, "1")
	_, last, _ := span(off)
	before := stateOf(off)
	off.n.SetOffline(true)
	moves(t, on, "m", 100)
	waitFor(t, "the host's log is cut past the offline player", func() bool {
		lo, _, n := span(h)
		return lo > last+1 && n <= 16+8+5
	})
	off.n.SetOffline(false)
	converged(t, ps, "101")
	if stateOf(off) == before {
		t.Fatal("the offline player caught up without the snapshot")
	}

	// The receipts came with the snapshot: as host, it still knows the moves.
	hr := h.replica()
	if err := wait(hr.LeadershipTransferToServer(raft.ServerID(off.self), raft.ServerAddress(off.self)), timeout); err != nil {
		t.Fatal(err)
	}
	if leader(t, ps...) != off {
		t.Fatal("the restored player is not host")
	}
	if r := off.serveSubmit(on[0].n.ID(), request{ID: "m98", Body: []byte("inc")}); r.Seq != 101 || string(off.State()) != "101" {
		t.Fatal("a retry of m98 at the restored host got", r)
	}
	if again := inc(t, off, "mine"); again != seq {
		t.Fatal("its own retry got", again, "not", seq)
	}
	inc(t, h, "after")
	converged(t, ps, "102")
	if err := off.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	records(t, ps, "102")
}

func TestHostChangeRightAfterCompactionLosesNothing(t *testing.T) {
	compactOften(t)
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inv := original(t, ps[0])
	moves(t, ps, "a", 60)
	crash(ps[2])
	moves(t, ps[:2], "b", 60)
	h := leader(t, ps[:2]...)
	waitFor(t, "the log is cut", func() bool { lo, _, _ := span(h); return lo > 60 })

	// A restarted player is a new server: nothing of the log it needs is left.
	c, err := restart(t, dirs[2], inv)
	if err != nil {
		t.Fatal(err)
	}
	if lo, _, _ := span(c); lo <= 60 || string(c.State()) != "120" {
		t.Fatal("rejoined with log from", lo, "state", string(c.State()))
	}
	if s := suffrage(t, h, c); s != raft.Voter {
		t.Fatal("the rejoined player is", s)
	}
	live := []*Player{ps[0], ps[1], c}
	moves(t, live, "c", 30)
	converged(t, live, "150")

	// Crash the host the moment it has compacted again.
	h = leader(t, live...)
	from, _, _ := span(h)
	moves(t, live, "d", 30)
	waitFor(t, "another cut", func() bool { lo, _, _ := span(h); return lo > from })
	crash(h)
	rest := without(live, h)
	leader(t, rest...)
	converged(t, rest, "180")
	inc(t, rest[0], "e")
	converged(t, rest, "181")
	if err := leader(t, rest...).Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	records(t, rest, "181")
}

func TestPlayerOfflineAtTheEndLearnsTheResult(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast) // production compaction
	h := leader(t, ps...)
	off := without(ps, h)[0]
	moves(t, ps, "m", 6)
	converged(t, ps, "6")
	off.n.SetOffline(true)
	moves(t, without(ps, off), "n", 6)
	if err := h.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Once over, the moves are flushed from memory: the log goes, the snapshot
	// keeps the result.
	for _, p := range without(ps, off) {
		waitFor(t, "an empty log", func() bool { _, _, n := span(p); return n == 0 })
	}
	off.n.SetOffline(false)
	waitFor(t, "the offline player learns the end", func() bool { return off.Status().Phase == room.Finished })
	waitFor(t, "an empty log on it too", func() bool { _, _, n := span(off); return n == 0 })
	records(t, ps, "12")
	// Its moves file stops before the end, and its replay says so.
	if ms, _, err := replayOf(off); err == nil || !strings.Contains(err.Error(), "are missing") || len(ms) < 6 {
		t.Fatal("the missing end was not reported:", len(ms), err)
	}
	for _, p := range without(ps, off) {
		if ms, _, err := replayOf(p); err != nil || len(ms) != 13 {
			t.Fatal(len(ms), err)
		}
	}
	for _, p := range ps {
		if v := p.Status().Version; v != 14 {
			t.Fatal("version", v)
		}
		if _, err := p.Submit(context.Background(), "m0", []byte("inc")); err == nil || err.Error() != room.ErrOver.Error() {
			t.Fatal(fmt.Sprint("a retry after the end: ", err))
		}
	}
}
