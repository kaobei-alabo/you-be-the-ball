package room_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func member(p string) room.Member { return room.Member{Peer: p, Node: p + "/node"} }

func lobby(t *testing.T, c room.Config) *room.Lobby {
	t.Helper()
	if c.Host == "" {
		c.Host = "host"
	}
	l, err := room.NewLobby(c)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func join(t *testing.T, l *room.Lobby, peer string) *room.Seat {
	t.Helper()
	id, secret, err := l.Invite()
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Join(member(peer), id, secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func started(t *testing.T, players ...string) *room.State {
	t.Helper()
	l := lobby(t, room.Config{Host: players[0], Players: len(players)})
	for _, p := range players[1:] {
		join(t, l, p)
	}
	setup, err := l.Start(member(players[0]))
	if err != nil {
		t.Fatal(err)
	}
	s, err := room.NewState(setup, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// submit does what the host does: decide, then apply the agreed entry.
func submit(t *testing.T, s *room.State, actor, id string) room.Result {
	t.Helper()
	e, done, err := s.Decide(actor, id, []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	if done != nil {
		return *done
	}
	return s.Apply(e)
}

func TestPlayerCountBounds(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 33} {
		if _, err := room.NewLobby(room.Config{Host: "host", Players: n}); err == nil {
			t.Fatalf("accepted %d players", n)
		}
	}
	for _, n := range []int{2, 32} {
		if _, err := room.NewLobby(room.Config{Host: "host", Players: n}); err != nil {
			t.Fatalf("rejected %d players: %v", n, err)
		}
	}
	if _, err := room.NewLobby(room.Config{Host: "host", Players: 2, TTL: room.MaxTTL + time.Second}); err == nil {
		t.Fatal("accepted a lifetime over 72h")
	}
}

func TestLobbyLastSeatRaceAndStartRules(t *testing.T) {
	l := lobby(t, room.Config{Players: 3})
	id, _, _ := l.Invite()
	if _, err := l.Start(member("host")); !errors.Is(err, room.ErrWaiting) {
		t.Fatal("started alone:", err)
	}
	join(t, l, "bob")
	var wg sync.WaitGroup
	var mu sync.Mutex
	seated := 0
	for _, p := range []string{"carol", "dave", "erin"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := l.Join(member(p), id, ""); err == nil {
				mu.Lock()
				seated++
				mu.Unlock()
			} else if !errors.Is(err, room.ErrFull) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if seated != 1 {
		t.Fatalf("%d players took the last seat", seated)
	}
	if _, err := l.Join(member("host"), id, ""); err == nil {
		t.Fatal("host joined its own room as a guest")
	}
	if _, err := l.Join(member("zed"), "wrong", ""); !errors.Is(err, room.ErrInvite) {
		t.Fatal("wrong room ID:", err)
	}
	if _, err := l.Start(member("bob")); err == nil {
		t.Fatal("a guest started the game")
	}
	setup, err := l.Start(member("host"))
	if err != nil {
		t.Fatal(err)
	}
	if setup.Players != 3 || len(setup.Roster) != 3 || setup.Roster[0].Peer != "host" || setup.Roster[1].Peer != "bob" {
		t.Fatalf("roster %+v", setup.Roster)
	}
	for _, m := range setup.Roster {
		if !m.Voter || m.Node == "" {
			t.Fatalf("member %+v", m)
		}
	}
	if _, err := l.Join(member("late"), id, ""); err == nil {
		t.Fatal("joined after the start")
	}
	if _, err := l.Start(member("host")); err == nil {
		t.Fatal("started twice")
	}
}

func TestLobbySeatFreedAndSuperseded(t *testing.T) {
	l := lobby(t, room.Config{Players: 2})
	old := join(t, l, "bob")
	fresh := join(t, l, "bob") // reconnect before the old connection is noticed
	old.Leave()
	if info, _ := l.Info(); len(info.Online) != 2 {
		t.Fatal("a stale connection freed the new seat:", info.Online)
	}
	fresh.Leave()
	if _, err := l.Start(member("host")); !errors.Is(err, room.ErrWaiting) {
		t.Fatal("started without the guest:", err)
	}
	join(t, l, "carol") // the freed seat can go to someone else
	if _, err := l.Join(member("bob"), mustID(l), ""); !errors.Is(err, room.ErrFull) {
		t.Fatal("room over capacity:", err)
	}
}

func mustID(l *room.Lobby) string { id, _, _ := l.Invite(); return id }

func TestPrivateLobbyAndListing(t *testing.T) {
	pub := lobby(t, room.Config{Players: 2})
	if info, err := pub.Listing(); err != nil || info.Host != "host" || len(info.Online) != 1 {
		t.Fatal(info, err)
	}
	join(t, pub, "bob")
	if _, err := pub.Listing(); err == nil {
		t.Fatal("listed a full lobby")
	}
	priv := lobby(t, room.Config{Players: 2, Private: true})
	id, secret, _ := priv.Invite()
	if secret == "" {
		t.Fatal("no secret")
	}
	if _, err := priv.Listing(); err == nil {
		t.Fatal("listed a private lobby")
	}
	if _, err := priv.Join(member("bob"), id, "guess"); !errors.Is(err, room.ErrInvite) {
		t.Fatal("joined without the secret:", err)
	}
	if _, err := priv.Join(member("bob"), id, secret); err != nil {
		t.Fatal(err)
	}
}

func TestLobbyExpiryCannotBeExtended(t *testing.T) {
	now := time.Unix(1000, 0)
	l := lobby(t, room.Config{Players: 2, TTL: time.Hour, Now: func() time.Time { return now }})
	before, _ := l.Info()
	join(t, l, "bob")
	now = now.Add(59 * time.Minute)
	if after, err := l.Info(); err != nil || !after.Expires.Equal(before.Expires) {
		t.Fatal("expiry moved:", after.Expires, err)
	}
	now = now.Add(time.Minute)
	if _, err := l.Start(member("host")); !errors.Is(err, room.ErrExpired) {
		t.Fatal("started an expired room:", err)
	}
	if _, _, err := l.Invite(); err == nil {
		t.Fatal("expired room still gives invites")
	}
}

func TestTwoPlayerGameOnlyHostMayBeChosenAsHost(t *testing.T) {
	s := started(t, "host", "bob")
	r := s.Setup().Roster
	if !r[0].Voter || r[1].Voter {
		t.Fatalf("two-player roster %+v", r)
	}
}

func TestRetryReturnsOriginalReceipt(t *testing.T) {
	s := started(t, "host", "bob")
	if v := s.Info().Version; v != 1 {
		t.Fatal("start is not version 1:", v)
	}
	first := submit(t, s, "bob", "bob-1")
	submit(t, s, "host", "host-1")
	again := submit(t, s, "bob", "bob-1")
	if first.Seq != 2 || again.Seq != 2 || !again.Repeat || string(s.View()) != "2" || s.Info().Version != 3 {
		t.Fatal(first, again, string(s.View()), s.Info().Version)
	}
	// IDs are scoped to the player: the host may use the same ID.
	if r := submit(t, s, "host", "bob-1"); r.Seq != 4 || r.Repeat {
		t.Fatal(r)
	}
}

func TestDecideRejectsWithoutChangingAnything(t *testing.T) {
	s := started(t, "host", "bob")
	for name, try := range map[string]func() error{
		"outsider":    func() error { _, _, err := s.Decide("mallory", "x", []byte("inc")); return err },
		"empty ID":    func() error { _, _, err := s.Decide("bob", "", []byte("inc")); return err },
		"long ID":     func() error { _, _, err := s.Decide("bob", strings.Repeat("x", 129), []byte("inc")); return err },
		"big payload": func() error { _, _, err := s.Decide("bob", "x", make([]byte, room.MaxPayload+1)); return err },
		"bad command": func() error { _, _, err := s.Decide("bob", "x", []byte("dec")); return err },
	} {
		if try() == nil {
			t.Fatal("accepted:", name)
		}
	}
	if s.Info().Version != 1 || string(s.View()) != "0" {
		t.Fatal("a rejected command changed the state")
	}
}

// Two hosts in a row may decide against the same version; only the first entry
// to be agreed on counts, and the other must be decided again.
func TestStaleEntryIsNotApplied(t *testing.T) {
	s := started(t, "host", "bob", "carol")
	a, _, err := s.Decide("bob", "bob-1", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := s.Decide("carol", "carol-1", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	if r := s.Apply(a); r.Seq != 2 {
		t.Fatal(r)
	}
	if r := s.Apply(b); !r.Stale || s.Info().Version != 2 || string(s.View()) != "1" {
		t.Fatal("stale entry applied:", r, s.Info().Version)
	}
	if r := submit(t, s, "carol", "carol-1"); r.Seq != 3 || r.Repeat {
		t.Fatal(r)
	}
	// The same entry agreed twice, as can happen across a host change, counts once.
	if r := s.Apply(a); !r.Repeat || r.Seq != 2 || string(s.View()) != "2" {
		t.Fatal(r, string(s.View()))
	}
}

func TestApplyRejectsInvalidEntriesIdentically(t *testing.T) {
	s := started(t, "host", "bob")
	snap, _ := s.Snapshot()
	twin, err := room.Restore(snap, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	bad := []room.Entry{
		{Kind: room.KindGame, Peer: "mallory", CommandID: "x", Body: []byte("+1"), Based: 1},
		{Kind: room.KindGame, Peer: "bob", CommandID: "", Body: []byte("+1"), Based: 1},
		{Kind: room.KindGame, Peer: "bob", CommandID: "x", Body: []byte("-1"), Based: 1},
		{Kind: room.KindGame, Peer: "bob", CommandID: "x", Body: make([]byte, room.MaxPayload+1), Based: 1},
		{Kind: "nonsense", Peer: "bob"},
	}
	for _, e := range bad {
		r, r2 := s.Apply(e), twin.Apply(e)
		if r.Err == "" || r != r2 {
			t.Fatalf("entry %+v: %+v vs %+v", e, r, r2)
		}
	}
	a, _ := s.Snapshot()
	b, _ := twin.Snapshot()
	if string(a) != string(b) || s.Info().Version != 1 {
		t.Fatal("rejected entries changed a copy")
	}
}

// A game may take any number of moves: what a copy holds does not grow with them.
func TestNoLimitOnMovesAndTheCopyStaysSmall(t *testing.T) {
	s := started(t, "host", "bob")
	const moves = 20000
	for i := 1; i <= moves; i++ {
		if r := submit(t, s, "bob", strconv.Itoa(i)); r.Seq != i+1 {
			t.Fatal(i, r)
		}
	}
	if string(s.View()) != strconv.Itoa(moves) || s.Info().Version != moves+1 {
		t.Fatal(string(s.View()), s.Info().Version)
	}
	snap, err := s.Snapshot()
	if err != nil || len(snap) > 64<<10 {
		t.Fatal("the copy grew with the number of moves:", len(snap), err)
	}
	// A retry is recognised among the player's most recent commands...
	recent := strconv.Itoa(moves - room.Receipts + 1)
	if r := submit(t, s, "bob", recent); !r.Repeat || r.Seq != moves-room.Receipts+2 {
		t.Fatal("recent receipt lost:", r)
	}
	if _, ok := s.Receipt("bob", strconv.Itoa(moves-room.Receipts)); ok {
		t.Fatal("window larger than room.Receipts")
	}
	// ...and each player has its own window: the host's first move is still known.
	first := submit(t, s, "host", "host-1")
	for i := 0; i < room.Receipts; i++ {
		submit(t, s, "bob", "more-"+strconv.Itoa(i))
	}
	if r := submit(t, s, "host", "host-1"); !r.Repeat || r.Seq != first.Seq {
		t.Fatal("one player's moves pushed out another's receipt:", r)
	}
	// An older one is forgotten: its ID then counts as a new command.
	if _, ok := s.Receipt("bob", "1"); ok {
		t.Fatal("receipts are kept without bound")
	}
}

func TestFinishAndExpireEndTheGame(t *testing.T) {
	for kind, phase := range map[string]room.Phase{room.KindFinish: room.Finished, room.KindExpire: room.Expired} {
		s := started(t, "host", "bob")
		submit(t, s, "bob", "bob-1")
		if r := s.Apply(s.End(kind, "host")); r.Seq != 3 || s.Info().Phase != phase {
			t.Fatal(kind, r, s.Info().Phase)
		}
		if r := s.Apply(s.End(room.KindFinish, "bob")); !r.Repeat || s.Info().Version != 3 || s.Info().Phase != phase {
			t.Fatal("ended twice:", r)
		}
		if _, _, err := s.Decide("bob", "bob-2", []byte("inc")); !errors.Is(err, room.ErrOver) {
			t.Fatal("command accepted after the end:", err)
		}
		if r := s.Apply(room.Entry{Kind: room.KindGame, Peer: "bob", CommandID: "bob-2", Body: []byte("+1"), Based: 3}); r.Err == "" {
			t.Fatal("entry applied after the end")
		}
		// Once the game is over only its result is kept, not its moves.
		if _, ok := s.Receipt("bob", "bob-1"); ok {
			t.Fatal("move detail kept after the end")
		}
		if string(s.View()) != "1" {
			t.Fatal("result lost:", string(s.View()))
		}
	}
}

// race ends itself when the counter reaches two.
type race struct{ counter.Game }

func (race) Finished(state []byte) bool { return string(state) == "2" }

func TestGameCanEndItself(t *testing.T) {
	l := lobby(t, room.Config{Players: 2})
	join(t, l, "bob")
	setup, _ := l.Start(member("host"))
	s, err := room.NewState(setup, race{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		e, _, err := s.Decide("bob", id, []byte("inc"))
		if err != nil {
			t.Fatal(err)
		}
		s.Apply(e)
	}
	if s.Info().Phase != room.Finished || s.Info().Version != 4 || string(s.View()) != "2" {
		t.Fatal(s.Info(), string(s.View()))
	}
}

func TestSnapshotRestoresAnIdenticalCopy(t *testing.T) {
	s := started(t, "host", "bob", "carol")
	submit(t, s, "bob", "bob-1")
	submit(t, s, "carol", "carol-1")
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	c, err := room.Restore(snap, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	if again := submit(t, c, "bob", "bob-1"); !again.Repeat || again.Seq != 2 {
		t.Fatal("receipts lost:", again)
	}
	for _, x := range []*room.State{s, c} {
		if r := submit(t, x, "host", "host-1"); r.Seq != 4 {
			t.Fatal(r)
		}
	}
	a, _ := s.Snapshot()
	b, _ := c.Snapshot()
	if string(a) != string(b) {
		t.Fatal("copies diverged")
	}
	for _, bad := range []string{"", "{}", `{"Setup":{"ID":"../x"}}`,
		strings.Replace(string(snap), `"Seq":2`, `"Seq":7`, 1),         // a receipt beyond the version
		strings.Replace(string(snap), `"Version":3`, `"Version":0`, 1), // no start
		strings.Replace(string(snap), `"ID":"bob-1"`, `"ID":""`, 1),    // a nameless command
		strings.Replace(string(snap), `"Phase":"playing"`, `"Phase":"x"`, 1)} {
		if _, err := room.Restore([]byte(bad), counter.Game{}); err == nil {
			t.Fatal("restored a bad snapshot:", bad)
		}
	}
}

func TestSetupIsValidated(t *testing.T) {
	good := started(t, "host", "bob").Setup()
	for name, change := range map[string]func(*room.Setup){
		"path in ID":     func(s *room.Setup) { s.ID = "../escape" },
		"empty ID":       func(s *room.Setup) { s.ID = "" },
		"missing player": func(s *room.Setup) { s.Roster = s.Roster[:1] },
		"same player":    func(s *room.Setup) { s.Roster[1].Peer = s.Roster[0].Peer },
		"no expiry":      func(s *room.Setup) { s.Expires = s.Created },
		"long expiry":    func(s *room.Setup) { s.Expires = s.Created.Add(room.MaxTTL + time.Second) },
	} {
		s := good
		s.Roster = append([]room.Member(nil), good.Roster...)
		change(&s)
		if _, err := room.NewState(s, counter.Game{}); err == nil {
			t.Fatal("accepted setup with", name)
		}
	}
	if _, err := room.NewState(good, nil); err == nil {
		t.Fatal("accepted a room without a game")
	}
}

func TestRecordIsWrittenWholeAndOnlyOnRequest(t *testing.T) {
	dir := t.TempDir()
	s := started(t, "host", "bob")
	submit(t, s, "bob", "bob-1")
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatal("playing wrote to disk:", entries)
	}
	file, err := room.WriteRecord(dir, s.Record())
	if err != nil {
		t.Fatal(err)
	}
	r, err := room.ReadRecord(file)
	if err != nil || r.Status != "interrupted" || r.Version != 2 || string(r.Result) != "1" || len(r.Players) != 2 {
		t.Fatal(r, err)
	}
	s.Apply(s.End(room.KindFinish, "host"))
	again, err := room.WriteRecord(dir, s.Record())
	if err != nil || again != file || filepath.Dir(file) != filepath.Join(dir, "games") {
		t.Fatal(again, err)
	}
	if r, _ = room.ReadRecord(file); r.Status != "finished" || r.Version != 3 || string(r.Result) != "1" {
		t.Fatal(r)
	}
	// The record is the result only: no move, command ID or mover appears in it.
	raw, _ := os.ReadFile(file)
	for _, detail := range []string{"bob-1", "Events", "CommandID", "+1"} {
		if strings.Contains(string(raw), detail) {
			t.Fatalf("record holds move detail %q: %s", detail, raw)
		}
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "games")); len(entries) != 1 {
		t.Fatal("leftover files:", entries)
	}
	if info, _ := os.Stat(file); info.Mode().Perm() != 0600 {
		t.Fatal("record readable by others:", info.Mode())
	}
	if _, err := room.WriteRecord(dir, room.Record{ID: "../escape"}); err == nil {
		t.Fatal("wrote outside the games folder")
	}
}

// scored keeps only a summary of its final state.
type scored struct{ counter.Game }

func (scored) Result(state []byte) []byte { return []byte("score=" + string(state)) }

func TestGameChoosesWhatTheRecordKeeps(t *testing.T) {
	l := lobby(t, room.Config{Players: 2})
	join(t, l, "bob")
	setup, _ := l.Start(member("host"))
	s, err := room.NewState(setup, scored{})
	if err != nil {
		t.Fatal(err)
	}
	e, _, _ := s.Decide("bob", "a", []byte("inc"))
	s.Apply(e)
	s.Apply(s.End(room.KindFinish, "host"))
	if r := s.Record(); string(r.Result) != "score=1" || r.Status != "finished" {
		t.Fatal(string(r.Result), r.Status)
	}
}
