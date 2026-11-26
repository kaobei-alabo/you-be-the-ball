package room_test

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func snap(t *testing.T, s *room.State) string {
	t.Helper()
	b, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Moves made at the same time are decided in a row, each on top of the one
// before, and agreed on together.
func TestDraftDecidesEveryonesMovesTogether(t *testing.T) {
	s := started(t, "host", "bob", "carol")
	before := snap(t, s)
	d := s.Draft()
	var entries []room.Entry
	for i := 0; i < 30; i++ {
		who := []string{"host", "bob", "carol"}[i%3]
		e, done, err := d.Decide(who, who+"-"+strconv.Itoa(i), []byte("inc"))
		if err != nil || done != nil || e.Based != i+1 {
			t.Fatal(i, e, done, err)
		}
		entries = append(entries, e)
	}
	if snap(t, s) != before {
		t.Fatal("drafting changed the state")
	}
	for i, e := range entries {
		if r := s.Apply(e); r.Seq != i+2 || r.Stale || r.Err != "" {
			t.Fatal(i, r)
		}
	}
	if string(s.View()) != "30" || s.Info().Version != 31 {
		t.Fatal(string(s.View()), s.Info().Version)
	}
}

// If an entry of the round is not applied after all, the ones decided after it
// are refused as stale on every copy, and decided again.
func TestDraftEntriesAfterAMissingOneAreStale(t *testing.T) {
	s := started(t, "host", "bob")
	twin, err := room.Restore([]byte(snap(t, s)), counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	d := s.Draft()
	a, _, _ := d.Decide("bob", "a", []byte("inc"))
	d.Decide("host", "b", []byte("inc")) // never arrives
	c, _, _ := d.Decide("bob", "c", []byte("inc"))
	for _, x := range []*room.State{s, twin} {
		if r := x.Apply(a); r.Seq != 2 {
			t.Fatal(r)
		}
		if r := x.Apply(c); !r.Stale {
			t.Fatal("an entry decided after a missing one was applied:", r)
		}
	}
	if snap(t, s) != snap(t, twin) || string(s.View()) != "1" {
		t.Fatal("copies differ")
	}
	if e, done, err := s.Draft().Decide("bob", "c", []byte("inc")); err != nil || done != nil || e.Based != 2 {
		t.Fatal("c could not be decided again:", e, done, err)
	}
}

func TestDraftAnswersBusyForWhatItCannotKnowYet(t *testing.T) {
	s := started(t, "host", "bob")
	submit(t, s, "bob", "old")
	d := s.Draft()
	e, _, err := d.Decide("bob", "x", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	// The same command again in the same round: its receipt does not exist yet.
	if _, _, err := d.Decide("bob", "x", []byte("inc")); !errors.Is(err, room.ErrBusy) {
		t.Fatal("same command twice in a round:", err)
	}
	// One that counted before the round is answered at once.
	if _, done, err := d.Decide("bob", "old", []byte("inc")); err != nil || done == nil || done.Seq != 2 {
		t.Fatal(done, err)
	}
	// A rejected command leaves the draft as it was.
	if _, _, err := d.Decide("mallory", "y", []byte("inc")); err == nil {
		t.Fatal("outsider accepted")
	}
	if _, _, err := d.Decide("bob", "bad", []byte("dec")); err == nil {
		t.Fatal("bad command accepted")
	}
	if next, _, err := d.Decide("host", "z", []byte("inc")); err != nil || next.Based != e.Based+1 {
		t.Fatal(next, err)
	}
	// After the end is drafted, nothing more can be answered until it is agreed.
	end, done, err := d.End(room.KindFinish, "host")
	if err != nil || done != nil || end.Based != 4 {
		t.Fatal(end, done, err)
	}
	if _, _, err := d.Decide("bob", "late", []byte("inc")); !errors.Is(err, room.ErrBusy) {
		t.Fatal("move after a drafted end:", err)
	}
	if _, _, err := d.End(room.KindFinish, "host"); !errors.Is(err, room.ErrBusy) {
		t.Fatal("second end in a round:", err)
	}
}

func TestDraftEndAfterTheGameIsOver(t *testing.T) {
	s := started(t, "host", "bob")
	e, _, err := s.Draft().End(room.KindFinish, "host")
	if err != nil || s.Apply(e).Seq != 2 {
		t.Fatal(e, err)
	}
	d := s.Draft()
	if _, done, err := d.End(room.KindFinish, "bob"); err != nil || done == nil || done.Seq != 2 {
		t.Fatal("a repeated finish:", done, err)
	}
	if _, _, err := d.End(room.KindExpire, "host"); !errors.Is(err, room.ErrOver) {
		t.Fatal("expiring a finished game:", err)
	}
	if _, _, err := d.Decide("bob", "late", []byte("inc")); !errors.Is(err, room.ErrOver) {
		t.Fatal("move after the end:", err)
	}
}

// A game that ends itself: the move that ends it is the last one of the round.
func TestDraftStopsAtAGameThatEndsItself(t *testing.T) {
	l := lobby(t, room.Config{Players: 2})
	join(t, l, "bob")
	setup, _ := l.Start(member("host"))
	s, err := room.NewState(setup, race{}) // over when the counter reaches two
	if err != nil {
		t.Fatal(err)
	}
	d := s.Draft()
	a, _, _ := d.Decide("bob", "a", []byte("inc"))
	b, _, err := d.Decide("host", "b", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Decide("bob", "c", []byte("inc")); !errors.Is(err, room.ErrBusy) {
		t.Fatal("move drafted after the one that ends the game:", err)
	}
	s.Apply(a)
	if r := s.Apply(b); r.Seq != 3 || s.Info().Phase != room.Finished || s.Info().Version != 4 {
		t.Fatal(r, s.Info())
	}
	if _, _, err := s.Draft().Decide("bob", "c", []byte("inc")); !errors.Is(err, room.ErrOver) {
		t.Fatal(err)
	}
}

// trail is a game whose state is the list of moves made so far and whose event
// is the whole list the host expected after the move. A move applied on any
// other state than the one it was decided on therefore loses or invents moves.
type trail struct{}

func (trail) Initial() []byte { return nil }
func (trail) Decide(state []byte, _ string, cmd []byte) ([]byte, error) {
	return append(append(state, ','), cmd...), nil
}
func (trail) Apply(_, event []byte) ([]byte, error) { return event, nil }

// Rounds are drafted from copies that may lag, entries go missing, and other
// hosts' entries get in between. Whatever happens, a move is applied only on
// the state it was decided on, and every copy agrees.
func TestAMoveIsOnlyAppliedOnTheStateItWasDecidedOn(t *testing.T) {
	players := []string{"host", "bob", "carol"}
	for seed := uint64(1); seed <= 300; seed++ {
		rnd := rand.New(rand.NewPCG(seed, 7))
		l, _ := room.NewLobby(room.Config{Host: "host", Players: 3})
		id, _, _ := l.Invite()
		for _, p := range players[1:] {
			l.Join(member(p), id, "")
		}
		setup, _ := l.Start(member("host"))
		truth, _ := room.NewState(setup, trail{})
		var log []room.Entry
		var applied []string
		snaps := [][]byte{[]byte(snap(t, truth))}
		next := 0
		draft := func(from *room.State, n int) (out []room.Entry) {
			d := from.Draft()
			for range n {
				next++
				if e, done, err := d.Decide(players[rnd.IntN(3)], "m"+strconv.Itoa(next), []byte("m"+strconv.Itoa(next))); err == nil && done == nil {
					out = append(out, e)
				}
			}
			return
		}
		commit := func(e room.Entry) {
			log = append(log, e)
			if r := truth.Apply(e); r.Seq > 0 && !r.Repeat {
				applied = append(applied, e.CommandID)
			}
			snaps = append(snaps, []byte(snap(t, truth)))
		}
		for range 25 {
			// The host drafts from what it has applied: sometimes not everything yet.
			from := truth
			if rnd.IntN(3) == 0 {
				from, _ = room.Restore(snaps[rnd.IntN(len(snaps))], trail{})
			}
			for _, e := range draft(from, 1+rnd.IntN(5)) {
				if rnd.IntN(5) == 0 {
					continue // this entry never makes it
				}
				if rnd.IntN(5) == 0 {
					for _, f := range draft(truth, 1+rnd.IntN(2)) { // another host's round
						commit(f)
					}
				}
				commit(e)
			}
		}
		want := ""
		for _, id := range applied {
			want += "," + id
		}
		if got := string(truth.View()); got != want {
			t.Fatalf("seed %d: the game holds %q but the moves that counted are %q", seed, got, want)
		}
		if truth.Info().Version != len(applied)+1 {
			t.Fatalf("seed %d: version %d after %d moves", seed, truth.Info().Version, len(applied))
		}
		// A copy that replays the log, restored from a snapshot on the way, agrees.
		copy2, _ := room.NewState(setup, trail{})
		cut := rnd.IntN(len(log) + 1)
		for i, e := range log {
			if i == cut {
				copy2, _ = room.Restore([]byte(snap(t, copy2)), trail{})
			}
			copy2.Apply(e)
		}
		if snap(t, copy2) != snap(t, truth) {
			t.Fatalf("seed %d: copies differ", seed)
		}
		if strings.Count(want, ",") != len(applied) {
			t.Fatal("bad test")
		}
	}
}
