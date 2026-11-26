package lan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func fails(t *testing.T, p *Player, id string, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if seq, err := p.Submit(ctx, id, []byte("inc")); err == nil {
		t.Errorf("move %s committed as %d", id, seq)
	}
}

func TestThreePlayersHostCrashHandsOver(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	for i, p := range ps {
		if seq := inc(t, p, fmt.Sprint("m", i)); p.Status().Version < seq {
			t.Fatal("a move returned before this player's copy held it")
		}
	}
	old := leader(t, ps...)
	rest := without(ps, old)
	seq := inc(t, rest[0], "r")
	converged(t, ps, "4")
	crash(old)
	h := leader(t, rest...)
	if h == old {
		t.Fatal("the crashed player is host")
	}
	inc(t, rest[1], "after")
	if again := inc(t, rest[0], "r"); again != seq {
		t.Fatalf("retry got %d, first try %d", again, seq)
	}
	converged(t, rest, "5")
}

func TestSuccessorIsNotAlwaysTheSamePlayer(t *testing.T) {
	// Each crash or graceful close picks one of two survivors; 30 identical
	// picks would happen by chance with probability 2^-29.
	for _, mode := range []string{"crash", "close"} {
		seen := map[int]bool{}
		for i := 0; i < 30 && len(seen) < 2; i++ {
			t.Run(fmt.Sprint(mode, i), func(t *testing.T) {
				ps := play(t, room.Config{Players: 3}, fast)
				if mode == "crash" {
					crash(ps[0])
				} else {
					ps[0].Close()
				}
				h := leader(t, ps[1:]...)
				seen[slices.Index(ps, h)] = true
			})
		}
		if len(seen) < 2 {
			t.Fatal("the same player took over every time after a", mode, seen)
		}
	}
}

func TestIsolatedHostCannotCommitAndReturnsAsPlayer(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	inc(t, ps[1], "a")
	old := leader(t, ps...)
	rest := without(ps, old)
	old.n.SetOffline(true)
	fails(t, old, "lost", time.Second)
	leader(t, rest...)
	inc(t, rest[0], "b")
	inc(t, rest[1], "c")
	old.n.SetOffline(false)
	converged(t, ps, "3")
	if old.Status().IsHost || leader(t, ps...) == old {
		t.Fatal("the isolated host is host again")
	}
}

func TestTwoPlayersHostStaysInCharge(t *testing.T) {
	ps := play(t, room.Config{Players: 2}, fast)
	h, g := ps[0], ps[1]
	g.n.SetOffline(true)
	inc(t, h, "h1")
	inc(t, h, "h2")
	g.n.SetOffline(false)
	converged(t, ps, "2")

	h.n.SetOffline(true)
	waitFor(t, "the guest pauses", func() bool { return g.Status().Paused })
	fails(t, g, "g1", 1500*time.Millisecond)
	if s := g.Status(); s.IsHost || !s.Paused {
		t.Fatal("the guest took over", s)
	}
	h.n.SetOffline(false)
	waitFor(t, "play resumes", func() bool { return !g.Status().Paused })
	inc(t, g, "g1")
	converged(t, ps, "3")
	if !h.Status().IsHost {
		t.Fatal("the host lost charge")
	}
}

func TestEvenSplitPausesBothSides(t *testing.T) {
	ps := play(t, room.Config{Players: 4}, fast)
	inc(t, ps[3], "a")
	converged(t, ps, "1")
	for _, x := range ps[:2] {
		for _, y := range ps[2:] {
			cut(x, y, true)
		}
	}
	waitFor(t, "no host anywhere", func() bool {
		for _, p := range ps {
			if p.Status().IsHost {
				return false
			}
		}
		return true
	})
	var wg sync.WaitGroup
	for i, p := range ps {
		wg.Go(func() { fails(t, p, fmt.Sprint("split", i), time.Second) })
	}
	wg.Wait()
	for _, p := range ps {
		if p.Status().Version != 2 || !p.Status().Paused {
			t.Fatal("a move committed during the split", p.Status())
		}
	}
	for _, x := range ps[:2] {
		for _, y := range ps[2:] {
			cut(x, y, false)
		}
	}
	leader(t, ps...)
	inc(t, ps[0], "b")
	waitFor(t, "identical state everywhere", func() bool {
		for _, p := range ps {
			if p.Status().Version != ps[0].Status().Version || !bytes.Equal(p.State(), ps[0].State()) {
				return false
			}
		}
		return true
	})
}

func TestRetriesAcrossHostChangesApplyOnce(t *testing.T) {
	ps := play(t, room.Config{Players: 5}, fast)
	type worker struct {
		stop, done chan struct{}
		acked      int
	}
	ws := make([]*worker, len(ps))
	for i, p := range ps {
		w := &worker{stop: make(chan struct{}), done: make(chan struct{})}
		ws[i] = w
		go func() {
			defer close(w.done)
			for n := 0; ; n++ {
				select {
				case <-w.stop:
					return
				default:
				}
				// Retry the same ID until it is acknowledged.
				id := fmt.Sprintf("w%d-%d", i, n)
				for end := time.Now().Add(30 * time.Second); ; {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_, err := p.Submit(ctx, id, []byte("inc"))
					cancel()
					if err == nil {
						break
					}
					if time.Now().After(end) {
						t.Errorf("%s never acknowledged: %v", id, err)
						return
					}
				}
				w.acked++
				if n == 700 { // keeps the test short
					<-w.stop
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	live := ps
	for range 2 {
		h := leader(t, live...)
		h.n.SetOffline(true)
		leader(t, without(live, h)...)
		time.Sleep(200 * time.Millisecond) // let moves pile up meanwhile
		h.n.SetOffline(false)
	}
	h := leader(t, live...)
	i := slices.Index(ps, h)
	close(ws[i].stop) // a crashed program acknowledges nothing: let it finish its move first
	<-ws[i].done
	crash(h)
	live = without(live, h)
	leader(t, live...)
	time.Sleep(200 * time.Millisecond)
	total := 0
	for j, w := range ws {
		if j != i {
			close(w.stop)
		}
		<-w.done
		total += w.acked
	}
	// Each acknowledged move counted once: the counter and the version say so.
	converged(t, live, strconv.Itoa(total))
	if v := live[0].Status().Version; v != total+1 {
		t.Errorf("version %d after %d moves", v, total)
	}
	t.Logf("%d moves acknowledged", total)
}

func TestFinishWritesTheSameRecordForEveryPlayer(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	for i, p := range ps {
		inc(t, p, fmt.Sprint("move-id-", i))
	}
	for _, p := range ps {
		if files, _ := os.ReadDir(p.o.Dir); len(files) != 0 {
			t.Fatal("written while playing:", files)
		}
	}
	if ps[1].Finish(context.Background()) == nil {
		t.Fatal("a guest finished the game")
	}
	h := leader(t, ps...)
	if err := h.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Finish(context.Background()); err != nil {
		t.Fatal("a retried finish:", err) // as after an unknown outcome
	}
	waitFor(t, "every record", func() bool {
		for _, p := range ps {
			if p.Status().Record == "" || p.Status().Phase != "finished" {
				return false
			}
		}
		return true
	})
	var first room.Record
	for i, p := range ps {
		file := p.Status().Record
		if file != filepath.Join(p.o.Dir, "games", p.id+".json") {
			t.Fatal("record at", file)
		}
		// Only the result is kept: no move, command ID or mover.
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, detail := range []string{"move-id-", "aW5j" /* "inc" */, "Events", "CommandID", "Peer", "Body", "Kind"} {
			if bytes.Contains(data, []byte(detail)) {
				t.Errorf("the record holds %q:\n%s", detail, data)
			}
		}
		r, err := room.ReadRecord(file)
		if err != nil || r.Status != "finished" {
			t.Fatal(r.Status, err)
		}
		if i == 0 {
			first = r
		} else if !reflect.DeepEqual(r, first) {
			t.Fatal("records differ", r, first)
		}
		fails(t, p, "late", time.Second)
		if _, err := p.Submit(context.Background(), "move-id-0", []byte("inc")); !errors.Is(err, room.ErrOver) {
			t.Fatal("a retry after the end:", err)
		}
	}
	if string(first.Result) != "3" || first.Version != 5 {
		t.Fatal("record", string(first.Result), first.Version)
	}
}

func TestClosingMidGameSavesAnInterruptedRecord(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	inc(t, ps[1], "a")
	converged(t, ps, "1")
	g := without(ps, leader(t, ps...))[0]
	g.Close()
	r, err := room.ReadRecord(g.Status().Record)
	if err != nil || r.Status != "interrupted" || r.Version != 2 || string(r.Result) != "1" {
		t.Fatal(r, err)
	}
	if g.Status().Phase != "closed" || g.Close() != nil {
		t.Fatal("closed player", g.Status())
	}
	rest := without(ps, g)
	inc(t, rest[0], "b")
	converged(t, rest, "2")
	for _, p := range rest {
		if p.Status().Record != "" {
			t.Fatal("record written while playing")
		}
	}
}

func TestExpiryEndsTheGameForEveryone(t *testing.T) {
	ps := play(t, room.Config{Players: 3, TTL: 3 * time.Second}, fast)
	seq := inc(t, ps[1], "a")
	waitFor(t, "expiry", func() bool {
		for _, p := range ps {
			if p.Status().Phase != "expired" || p.Status().Record == "" {
				return false
			}
		}
		return true
	})
	// Once the game is over its receipts are gone too.
	if again, err := ps[1].Submit(context.Background(), "a", []byte("inc")); !errors.Is(err, room.ErrExpired) {
		t.Fatal("a retry after the deadline got", again, err, "first try", seq)
	}
	for _, p := range ps {
		if _, err := p.Submit(context.Background(), "late", []byte("inc")); !errors.Is(err, room.ErrExpired) {
			t.Fatal("late move:", err)
		}
		if r, err := room.ReadRecord(p.Status().Record); err != nil || r.Status != "expired" {
			t.Fatal(r.Status, err)
		}
	}
}

func TestLeavingHostHandsOverQuickly(t *testing.T) {
	// With a 2 s heartbeat a crash goes unnoticed for at least 2 s.
	slow := Timing{Heartbeat: 2 * time.Second, Lease: time.Second, Commit: 2 * time.Millisecond}
	ps := play(t, room.Config{Players: 3}, slow)
	inc(t, ps[1], "a")
	h := leader(t, ps...)
	begin := time.Now()
	h.Close()
	rest := without(ps, h)
	leader(t, rest...)
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("handover took %v", d)
	}
	inc(t, rest[0], "b")
	converged(t, rest, "2")
}

func TestTwoPlayersHostClosingEndsTheGameForBoth(t *testing.T) {
	ps := play(t, room.Config{Players: 2}, fast)
	h, g := ps[0], ps[1]
	inc(t, g, "g1")
	inc(t, h, "h1")
	h.Close()
	for _, p := range ps {
		r, err := room.ReadRecord(p.Status().Record)
		if err != nil || r.Status != "interrupted" || r.Version != 3 {
			t.Fatal(r, err)
		}
		if s := p.Status(); s.Phase != Closed || s.IsHost {
			t.Fatal(s)
		}
	}
}

func TestSecondSetupStopsTheGuest(t *testing.T) {
	fake := newNode(t, t.TempDir()) // a creator that sends Setup twice
	quit := make(chan struct{})
	defer close(quit)
	fake.h.SetStreamHandler(lobbyProto, func(s network.Stream) {
		defer s.Reset()
		var q request
		if recv(s, &q) != nil {
			return
		}
		now := time.Now()
		me := fake.ID().String()
		setup := room.Setup{ID: q.Room, Players: 2, Created: now, Expires: now.Add(time.Hour), Roster: []room.Member{
			{Peer: me, Node: address(fake.ID(), q.Room, "x"), Voter: true}, {Peer: s.Conn().RemotePeer().String(), Node: q.Node}}}
		_ = send(s, reply{Lobby: &room.LobbyInfo{ID: q.Room, Host: me, Players: 2}})
		_ = send(s, reply{Setup: &setup})
		time.Sleep(100 * time.Millisecond)
		_ = send(s, reply{Setup: &setup})
		<-quit
	})
	g, err := join(t, newNode(t, t.TempDir()), Invite{Room: room.Token(), Host: peer.AddrInfo{ID: fake.ID(), Addrs: fake.h.Addrs()}}, Options{Timing: fast})
	if err != nil {
		t.Fatal(err)
	}
	// Without a word from the creator the guest gives up after 10 s anyway.
	for end := time.Now().Add(5 * time.Second); g.Status().Phase != Closed; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("a second Setup did not stop the guest")
		}
	}
}
