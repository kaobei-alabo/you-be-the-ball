package room_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// play applies n moves by bob and logs each, as a player's program does.
func play(t *testing.T, s *room.State, m *room.Moves, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		id := "bob-" + strconv.Itoa(i)
		e, _, err := s.Decide("bob", id, []byte("inc"))
		if err != nil {
			t.Fatal(err)
		}
		if r := s.Apply(e); r.Seq > 0 && !r.Repeat {
			m.Add(room.Move{Seq: r.Seq, Peer: e.Peer, ID: e.CommandID, Body: e.Body})
		}
	}
}

func replay(t *testing.T, file string) (states []string, moves []room.Move, err error) {
	t.Helper()
	_, err = room.Replay(counter.Game{}, file, func(m room.Move, state []byte) error {
		states, moves = append(states, string(state)), append(moves, m)
		return nil
	})
	return
}

func TestMovesAreKeptAndReplayToTheSameResult(t *testing.T) {
	dir := t.TempDir()
	s := started(t, "host", "bob")
	m, err := room.OpenMoves(dir, s.Setup())
	if err != nil {
		t.Fatal(err)
	}
	play(t, s, m, 1, 300)
	end := s.Apply(s.End(room.KindFinish, "host"))
	m.Add(room.Move{Seq: end.Seq, Kind: room.KindFinish, Peer: "host"})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "games", s.Info().ID+".moves")
	info, err := room.Replay(counter.Game{}, file, func(room.Move, []byte) error { return nil })
	if err != nil || info.ID != s.Info().ID || len(info.Players) != 2 || !info.Expires.Equal(s.Info().Expires) {
		t.Fatal(info, err)
	}
	states, moves, err := replay(t, file)
	if err != nil || len(moves) != 301 {
		t.Fatal(len(moves), err)
	}
	for i, st := range states[:300] {
		if st != strconv.Itoa(i+1) || moves[i].Seq != i+2 || moves[i].Peer != "bob" || moves[i].ID != "bob-"+strconv.Itoa(i+1) {
			t.Fatal("step", i, st, moves[i])
		}
	}
	if last := moves[300]; last.Kind != room.KindFinish || last.Seq != s.Info().Version || states[300] != string(s.View()) {
		t.Fatal("the replay does not end where the game did:", last, states[300])
	}
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0600 {
		t.Fatal("moves readable by others:", fi.Mode())
	}
}

// A restarted player replays the game from its start: nothing is written twice,
// and a line cut short by the crash is dropped.
func TestMovesFileIsContinuedAfterARestart(t *testing.T) {
	dir := t.TempDir()
	s := started(t, "host", "bob")
	file := filepath.Join(dir, "games", s.Info().ID+".moves")
	m, _ := room.OpenMoves(dir, s.Setup())
	play(t, s, m, 1, 5)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(`{"Seq":7,"Peer":"bo`) // the crash
	f.Close()

	fresh := started(t, "host", "bob") // the restarted program applies the same moves from the start
	setup := s.Setup()
	m, err := room.OpenMoves(dir, setup)
	if err != nil {
		t.Fatal(err)
	}
	play(t, fresh, m, 1, 8) // the first five are already in the file
	m.Close()
	states, moves, err := replay(t, file)
	if err != nil || len(moves) != 8 || states[7] != "8" {
		t.Fatal(len(moves), states, err)
	}
	raw, _ := os.ReadFile(file)
	if strings.Count(string(raw), "\n") != 9 || strings.Contains(string(raw), `"bo`+"\n") {
		t.Fatalf("file not clean:\n%s", raw)
	}
	// A crash may leave half a line at the end: the replay stops before it.
	os.Truncate(file, int64(len(raw)-10))
	if states, moves, err = replay(t, file); err != nil || len(moves) != 7 || states[6] != "7" {
		t.Fatal(len(moves), err)
	}
	// A file holding only a torn first line starts over.
	os.WriteFile(file, []byte(`{"ID":"`), 0600)
	m, err = room.OpenMoves(dir, setup)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	if info, err := room.Replay(counter.Game{}, file, func(room.Move, []byte) error { return nil }); err != nil || info.ID != setup.ID {
		t.Fatal(info, err)
	}
	// Not a moves file at all: refused, not a panic.
	os.WriteFile(file, []byte(strings.Repeat("x", 70<<10)), 0600)
	if _, err := room.OpenMoves(dir, setup); err == nil {
		t.Fatal("continued a file with no line end")
	}
}

func TestReplayReportsMovesThisPlayerMissed(t *testing.T) {
	dir := t.TempDir()
	s := started(t, "host", "bob")
	m, _ := room.OpenMoves(dir, s.Setup())
	play(t, s, m, 1, 3)
	m.Add(room.Move{Seq: 40, Peer: "bob", ID: "later", Body: []byte("+1")}) // caught up from a snapshot
	m.Close()
	states, _, err := replay(t, filepath.Join(dir, "games", s.Info().ID+".moves"))
	if err == nil || !strings.Contains(err.Error(), "moves 5 to 39 are missing") || len(states) != 3 {
		t.Fatal(states, err)
	}
	// Brought up to date from a snapshot of the finished game: only the
	// record tells the moves at the end are missing.
	s = started(t, "host", "bob")
	m, _ = room.OpenMoves(dir, s.Setup())
	play(t, s, m, 1, 3)
	m.Close()
	play(t, s, m, 4, 1) // closed: not kept
	s.Apply(s.End(room.KindFinish, "host"))
	if _, err := room.WriteRecord(dir, s.Record()); err != nil {
		t.Fatal(err)
	}
	if states, _, err = replay(t, filepath.Join(dir, "games", s.Info().ID+".moves")); err == nil || !strings.Contains(err.Error(), "moves 5 to 6 are missing") || len(states) != 3 {
		t.Fatal(states, err)
	}
	if _, err := room.Replay(counter.Game{}, filepath.Join(dir, "nothing"), nil); err == nil {
		t.Fatal("replayed a missing file")
	}
	os.WriteFile(filepath.Join(dir, "junk"), []byte("hello\n"), 0600)
	if _, err := room.Replay(counter.Game{}, filepath.Join(dir, "junk"), nil); err == nil {
		t.Fatal("replayed a file that is not a moves file")
	}
}

func TestMovesArePurgedAfterTheirTimeAndResultsNever(t *testing.T) {
	dir := t.TempDir()
	games := filepath.Join(dir, "games")
	now := time.Now()
	make := func(id string, age time.Duration, result string) {
		s := started(t, "host", "bob")
		setup := s.Setup()
		setup.ID = id
		m, err := room.OpenMoves(dir, setup)
		if err != nil {
			t.Fatal(err)
		}
		m.Close()
		os.Chtimes(filepath.Join(games, id+".moves"), now.Add(-age), now.Add(-age))
		if result != "" {
			r := s.Record()
			r.ID, r.Status = id, result
			if result == "interrupted-and-expired" { // the room is over: nobody can rejoin
				r.Status, r.Expires = "interrupted", now.Add(-time.Minute)
			}
			if _, err := room.WriteRecord(dir, r); err != nil {
				t.Fatal(err)
			}
			os.Chtimes(filepath.Join(games, id+".json"), now.Add(-age), now.Add(-age))
		}
	}
	make("fresh", time.Hour, "finished")
	make("old", 73*time.Hour, "finished")
	make("oldcrashed", 73*time.Hour, "")         // no result, but no room lives that long
	make("midgame", 2*time.Hour, "")             // no result yet: may still be going
	make("rejoined", 2*time.Hour, "interrupted") // the player may have rejoined it
	make("done", 2*time.Hour, "expired")
	make("left", 2*time.Hour, "interrupted-and-expired")
	if n, err := room.PurgeMoves(dir, 72*time.Hour, now); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if n, err := room.PurgeMoves(dir, time.Hour+time.Minute, now); err != nil || n != 2 { // a shorter time: "done" and "left" go, "midgame" and "rejoined" stay
		t.Fatal(n, err)
	}
	left, _ := os.ReadDir(games)
	var names []string
	for _, e := range left {
		names = append(names, e.Name())
	}
	if got := strings.Join(names, " "); got != "done.json fresh.json fresh.moves left.json midgame.moves old.json rejoined.json rejoined.moves" {
		t.Fatal(got)
	}
	if n, err := room.PurgeMoves(t.TempDir(), time.Hour, now); n != 0 || err != nil {
		t.Fatal("no games folder:", n, err)
	}
}

func BenchmarkMovesAdd(b *testing.B) {
	l, _ := room.NewLobby(room.Config{Host: "host", Players: 2})
	id, _, _ := l.Invite()
	l.Join(member("bob"), id, "")
	setup, _ := l.Start(member("host"))
	m, err := room.OpenMoves(b.TempDir(), setup)
	if err != nil {
		b.Fatal(err)
	}
	body := []byte("+1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(room.Move{Seq: i + 2, Peer: "12D3KooWB6pW71dJBuoy6RRPXzqTApoiqEK7QJNvgJdcB3JqSqrf", ID: "bob-1234567", Body: body})
	}
	b.StopTimer()
	if err := m.Close(); err != nil {
		b.Fatal(err)
	}
}
