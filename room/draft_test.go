package room_test

import (
	"errors"
	"strconv"
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
