package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/internal/home"
	"github.com/kaobei-alabo/you-be-the-ball/lan"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func fakeHome(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	root, err := home.Root()
	if err != nil {
		t.Fatal(err)
	}
	if root != filepath.Join(dir, ".you-be-the-ball") {
		t.Fatal(root)
	}
	return root
}

func TestHomeProfilesIgnoreWorkingDirectory(t *testing.T) {
	root := fakeHome(t)
	for range 2 {
		cwd := t.TempDir()
		t.Chdir(cwd)
		for _, mode := range []string{"host", "join", "find"} {
			var args []string
			if mode == "join" {
				args = []string{"invite"}
			}
			o, err := parseOptions(io.Discard, root, mode, args)
			if err != nil || o.dir != filepath.Join(root, mode) {
				t.Fatalf("%s: %+v, %v", mode, o, err)
			}
		}
		entries, err := os.ReadDir(cwd)
		if err != nil || len(entries) != 0 {
			t.Fatal("unexpected working directory state", entries, err)
		}
	}
	custom, err := parseOptions(io.Discard, root, "host", []string{"-profile", "colleague-test"})
	if err != nil || custom.dir != filepath.Join(root, "colleague-test") {
		t.Fatal(custom, err)
	}
	for _, args := range [][]string{{"-profile", "../not-a-profile"}, {"-profile", "."}, {"-profile", ""}, {"-profile", "alice", "-dir", "."}, {"-dir", ""}} {
		if _, err := parseOptions(io.Discard, root, "host", args); err == nil {
			t.Fatal("accepted", args)
		}
	}
	explicit := t.TempDir()
	custom, err = parseOptions(io.Discard, root, "host", []string{"-dir", explicit})
	if err != nil || custom.dir != explicit {
		t.Fatal(custom, err)
	}
}

func TestHomeConfigAndFlagPrecedence(t *testing.T) {
	root := fakeHome(t)
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"players":4,"private":true,"mdns":false,"host_listen":"/ip4/127.0.0.1/tcp/42345"}`), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := parseOptions(io.Discard, root, "host", nil)
	if err != nil || !h.private || h.mdns || h.players != 4 || h.listen != "/ip4/127.0.0.1/tcp/42345" {
		t.Fatal(h, err)
	}
	h, err = parseOptions(io.Discard, root, "host", []string{"-players", "3", "-private=false", "-mdns"})
	if err != nil || h.private || !h.mdns || h.players != 3 {
		t.Fatal(h, err)
	}
	for flags, want := range map[string]string{"-players 1": "-players must be 2..32", "-players 33": "-players must be 2..32", "-ttl 100h": "-ttl must be in (0,72h]"} {
		if _, err := parseOptions(io.Discard, root, "host", strings.Fields(flags)); err == nil || err.Error() != want {
			t.Fatal(flags, err)
		}
	}
	peer, err := parseOptions(io.Discard, root, "join", []string{"invite"})
	if err != nil || peer.listen != "/ip4/0.0.0.0/tcp/0" {
		t.Fatal(peer, err)
	}
	if h.keep != 72*time.Hour {
		t.Fatal("default keep-moves", h.keep)
	}
	if err := os.WriteFile(path, []byte(`{"keep_moves":"1h30m"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if h, err = parseOptions(io.Discard, root, "host", nil); err != nil || h.keep != 90*time.Minute {
		t.Fatal(h.keep, err)
	}
	if h, err = parseOptions(io.Discard, root, "join", []string{"-keep-moves", "0", "invite"}); err != nil || h.keep != 0 {
		t.Fatal(h.keep, err)
	}
	if _, err = parseOptions(io.Discard, root, "host", []string{"-keep-moves", "-1h"}); err == nil {
		t.Fatal("accepted a negative -keep-moves")
	}
	for _, invalid := range []string{`{`, `{"typo":true}`, `{} {}`, `null`, `{"players":1}`, `{"keep_moves":"3 days"}`, `{"keep_moves":72}`, `{"keep_moves":"-1h"}`} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseOptions(io.Discard, root, "host", nil); err == nil {
			t.Fatalf("accepted invalid config: %s", invalid)
		}
	}
}

// An old profile keeps its identity; its room.json moves along but is not used.
func TestMigrateMovesIdentityWithoutOverwriting(t *testing.T) {
	root := fakeHome(t)
	old := filepath.Join(t.TempDir(), ".p2p-host")
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"identity.key": "fixture-key", "room.json": "{}"} {
		if err := os.WriteFile(filepath.Join(old, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := run([]string{"migrate", old}, nil, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old dir still present", err)
	}
	o, err := parseOptions(io.Discard, root, "host", nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(filepath.Join(o.dir, "identity.key"))
	if err != nil || string(key) != "fixture-key" {
		t.Fatal("identity was not preserved", err)
	}
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "identity.key"), []byte("different-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"migrate", old}, nil, io.Discard, io.Discard); err == nil {
		t.Fatal("allowed overwrite")
	}
	key, err = os.ReadFile(filepath.Join(o.dir, "identity.key"))
	if err != nil || string(key) != "fixture-key" {
		t.Fatal("destination key changed")
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatal("source lost", err)
	}
}

// cli is one player's run() driven through pipes, as if typed in a terminal.
type cli struct {
	t      *testing.T
	name   string
	in     *io.PipeWriter
	answer chan string // every line except notices and request-ID hints
	invite string
	mu     sync.Mutex
	lines  []string
}

func fastCLI(t *testing.T) {
	oldT, oldM := timing, moveTimeout
	timing = lan.Timing{Heartbeat: 100 * time.Millisecond, Lease: 50 * time.Millisecond, Commit: 2 * time.Millisecond}
	moveTimeout = 3 * time.Second
	t.Cleanup(func() { timing, moveTimeout = oldT, oldM })
}

func launch(t *testing.T, name string, args ...string) *cli {
	t.Helper()
	args = append(args[:1:1], append([]string{"-profile", name, "-listen", "/ip4/127.0.0.1/tcp/0", "-mdns=false"}, args[1:]...)...)
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &cli{t: t, name: name, in: inW, answer: make(chan string, 100)}
	done := make(chan error, 1)
	go func() {
		err := run(args, inR, outW, io.Discard)
		outW.Close()
		done <- err
	}()
	go func() {
		s := bufio.NewScanner(outR)
		for s.Scan() {
			line := s.Text()
			c.mu.Lock()
			c.lines = append(c.lines, line)
			c.mu.Unlock()
			if !strings.HasPrefix(line, "* ") && !strings.HasPrefix(line, "request ID") {
				c.answer <- line
			}
		}
		close(c.answer)
	}()
	t.Cleanup(func() {
		inW.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
		case <-time.After(30 * time.Second):
			t.Errorf("%s did not quit", name)
		}
	})
	for {
		line := c.next()
		if strings.HasPrefix(line, "Invite: ") {
			c.invite = strings.TrimPrefix(line, "Invite: ")
		}
		if strings.HasPrefix(line, "Commands:") {
			return c
		}
	}
}

func (c *cli) next() string {
	c.t.Helper()
	select {
	case line, ok := <-c.answer:
		if !ok {
			c.t.Fatalf("%s exited", c.name)
		}
		return line
	case <-time.After(40 * time.Second):
		c.t.Fatalf("%s: no answer", c.name)
	}
	return ""
}

// do types one command and returns its answer.
func (c *cli) do(cmd string) string {
	c.t.Helper()
	if _, err := fmt.Fprintln(c.in, cmd); err != nil {
		c.t.Fatal(err)
	}
	return c.next()
}

// until repeats cmd until its answer contains every want, for up to 30 s.
func (c *cli) until(cmd string, want ...string) string {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		line := c.do(cmd)
		ok := true
		for _, w := range want {
			ok = ok && strings.Contains(line, w)
		}
		if ok {
			return line
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s %q: got %q, want %q", c.name, cmd, line, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *cli) mark() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.lines)
}

// notice waits for a printed line starting with prefix after mark.
func (c *cli) notice(mark int, prefix string) string {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, l := range c.lines[mark:] {
			if strings.HasPrefix(l, prefix) {
				c.mu.Unlock()
				return l
			}
		}
		c.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%s: no %q", c.name, prefix)
	return ""
}

func (c *cli) count(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, l := range c.lines {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func TestThreePlayersHostChange(t *testing.T) {
	root := fakeHome(t)
	fastCLI(t)
	alice := launch(t, "alice", "host", "-players", "3")
	bob := launch(t, "bob", "join", alice.invite)
	carol := launch(t, "carol", "join", alice.invite)
	all := []*cli{alice, bob, carol}
	alice.until("status", "phase=lobby online=3/3 expires=")
	if got := bob.do("start"); got != "error: only the host can start" {
		t.Fatal(got)
	}
	if got := alice.do("start"); got != "Started" {
		t.Fatal(got)
	}
	for _, c := range all {
		c.notice(0, "* game started")
	}
	bob.until("inc b1", "committed b1 at v2")
	carol.until("inc c1", "committed c1 at v3")

	marks := []int{bob.mark(), carol.mark()}
	if got := alice.do("offline"); got != "Offline" {
		t.Fatal(got)
	}
	for i, c := range all[1:] {
		c.notice(marks[i], "* host is now")
	}
	alice.notice(0, "* paused: no host reachable")
	for _, c := range all[1:] {
		if n := c.count("* paused"); n != 0 {
			t.Fatal(c.name, "told of a pause during a quick host change")
		}
	}
	bob.until("inc b2", "committed b2 at v4")
	bob.until("inc b1", "committed b1 at v2") // a retry returns the original receipt
	if got := alice.do("online"); got != "Online" {
		t.Fatal(got)
	}
	if line := alice.until("status", "phase=playing", "version=4 counter=3"); strings.Contains(line, "host=me") {
		t.Fatal("the old host is host again:", line)
	}

	host := bob
	if !strings.Contains(bob.until("status", "phase=playing"), "host=me") {
		host = carol
	}
	if got := host.do("finish"); got != "Finished" {
		t.Fatal(got)
	}
	for _, c := range all {
		line := c.until("status", "phase=finished", "version=5 counter=3", "record=")
		rec := line[strings.Index(line, "record=")+len("record="):]
		if filepath.Dir(filepath.Dir(rec)) != filepath.Join(root, c.name) {
			t.Fatal("record outside the profile:", rec)
		}
		if _, err := os.Stat(rec); err != nil {
			t.Fatal(err)
		}
		c.notice(0, "* game finished; record saved to "+rec)
		if n := c.count("* game started"); n != 1 {
			t.Fatalf("%s: %d start notices", c.name, n)
		}
		// Every player replays the whole game.
		id := strings.TrimSuffix(filepath.Base(rec), ".json")
		if got := cmdOut(t, "replay", "-profile", c.name); !strings.HasPrefix(got, id+"  ") || !strings.HasSuffix(got, "  finished\n") {
			t.Fatalf("%s: replay list %q", c.name, got)
		}
		got := strings.Split(cmdOut(t, "replay", "-profile", c.name, id), "\n")
		r, err := room.ReadRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 7 || !strings.HasPrefix(got[0], "Room "+id+", players ") || !strings.HasPrefix(got[1], "v2 ") || !strings.HasSuffix(got[1], " b1 counter=1") ||
			!strings.HasSuffix(got[3], " b2 counter="+string(r.Result)) || got[4] != "v5 finish" || got[5] != "3 moves replayed" {
			t.Fatalf("%s: replay %q", c.name, got)
		}
	}
}

// cmdOut runs a command that exits by itself and returns what it printed.
func cmdOut(t *testing.T, args ...string) string {
	t.Helper()
	var b strings.Builder
	if err := run(args, nil, &b, io.Discard); err != nil {
		t.Fatal(args, err)
	}
	return b.String()
}

// With two players only the creator hosts: the guest never takes over.
func TestTwoPlayersHostStaysInCharge(t *testing.T) {
	root := fakeHome(t)
	fastCLI(t)
	alice := launch(t, "alice", "host", "-players", "2", "-keep-moves", "0")
	bob := launch(t, "bob", "join", alice.invite)
	alice.until("status", "online=2/2")
	if got := alice.do("start"); got != "Started" {
		t.Fatal(got)
	}
	bob.notice(0, "* game started")

	bob.do("offline")
	alice.until("inc a1", "committed a1 at v2")
	bob.do("online")
	bob.until("status", "version=2 counter=1")

	alice.do("offline")
	if got := bob.do("inc b1"); !strings.HasPrefix(got, "error: ") {
		t.Fatal(got)
	}
	bob.until("status", "phase=playing host=none version=2", "paused")
	alice.do("online")
	bob.until("inc b1", "committed b1 at v3")
	alice.until("status", "host=me version=3 counter=2")

	// The host quits: the game ends for both.
	m := bob.mark()
	fmt.Fprintln(alice.in, "quit")
	bob.notice(m, "* room closed")
	bob.until("status", "phase=closed")
	for name, want := range map[string]int{"alice": 0, "bob": 1} { // alice keeps no moves
		if files, _ := filepath.Glob(filepath.Join(root, name, "games", "*.moves")); len(files) != want {
			t.Fatal(name, files)
		}
	}
}

func TestGuestOfAClosedLobby(t *testing.T) {
	fakeHome(t)
	fastCLI(t)
	alice := launch(t, "alice", "host", "-players", "3")
	bob := launch(t, "bob", "join", alice.invite)
	alice.until("status", "online=2/3")
	fmt.Fprintln(alice.in, "quit")
	bob.notice(0, "* room closed")
	if got := bob.do("status"); got != "phase=closed" {
		t.Fatal(got)
	}
}

// quit types "quit" and waits until run returns.
func (c *cli) quit() {
	c.t.Helper()
	fmt.Fprintln(c.in, "quit")
	for {
		select {
		case _, ok := <-c.answer:
			if !ok {
				return
			}
		case <-time.After(30 * time.Second):
			c.t.Fatalf("%s did not quit", c.name)
		}
	}
}

// A player comes back with "join" on the same profile. Its REPL is left with
// "quit", a graceful leave: run() cannot be killed abruptly in a test. That
// keeps its raft server in the game just as a crash does.
func TestPlayerRejoinsWithTheSameProfile(t *testing.T) {
	fakeHome(t)
	fastCLI(t)
	alice := launch(t, "alice", "host", "-players", "3")
	bob := launch(t, "bob", "join", alice.invite)
	carol := launch(t, "carol", "join", alice.invite)
	alice.until("status", "online=3/3")
	if got := alice.do("invite"); !strings.HasPrefix(got, "Invite: ") {
		t.Fatal(got)
	}
	if got := bob.do("invite"); got != "error: only the room's creator has its invite" {
		t.Fatal(got)
	}
	if got := alice.do("start"); got != "Started" {
		t.Fatal(got)
	}
	carol.notice(0, "* game started")
	bob.until("inc b1", "committed b1 at v2")
	carol.until("inc c1", "committed c1 at v3")

	carol.quit()
	carol = launch(t, "carol", "join", alice.invite)
	if carol.count("Rejoined at v3") != 1 {
		t.Fatal("no rejoin greeting")
	}
	carol.until("status", "phase=playing host=", "version=3 counter=2")
	carol.until("inc c2", "committed c2 at v4")
	bob.until("status", "version=4 counter=3")

	// Any player's invite works after the start.
	token, ok := strings.CutPrefix(bob.do("invite"), "Invite: ")
	if !ok {
		t.Fatal("no invite from bob")
	}
	carol.quit()
	carol = launch(t, "carol", "join", token)
	carol.until("inc c3", "committed c3 at v5")
	alice.until("status", "version=5 counter=4")
	if carol.count("Rejoined at v4") != 1 || carol.count("* game started") != 0 {
		t.Fatal("wrong rejoin output")
	}
}

// moves writes a moves file for room id in profile, n moves long, optionally
// with a result; it returns the file.
func moves(t *testing.T, root, profile, id string, n int, result bool) string {
	t.Helper()
	dir := filepath.Join(root, profile)
	now := time.Now()
	setup := room.Setup{ID: id, Players: 2, Created: now, Expires: now.Add(time.Hour), Roster: []room.Member{{Peer: "peer-aaaaaaaa"}, {Peer: "peer-bbbbbbbb"}}}
	m, err := room.OpenMoves(dir, setup)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		m.Add(room.Move{Seq: i + 2, Peer: "peer-bbbbbbbb", ID: fmt.Sprint("b", i), Body: []byte("+1")})
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if result {
		if _, err := room.WriteRecord(dir, room.Record{ID: id, Status: "finished"}); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "games", id+".moves")
}

func TestReplayListsPlaysAndPurges(t *testing.T) {
	root := fakeHome(t)
	if got := cmdOut(t, "replay"); got != "No replays in "+root+".\n" {
		t.Fatal(got)
	}
	old := moves(t, root, "host", "old", 2, true)
	moves(t, root, "join", "kept", 2, false)
	both := moves(t, root, "host", "both", 1, true)
	moves(t, root, "join", "both", 1, true)
	day := 24 * time.Hour
	for f, age := range map[string]time.Duration{old: 4 * day, both: day} {
		if err := os.Chtimes(f, time.Now().Add(-age), time.Now().Add(-age)); err != nil {
			t.Fatal(err)
		}
	}
	// "old" is past the 3 days: gone, its result kept. Newest first.
	lines := strings.Split(strings.TrimSpace(cmdOut(t, "replay")), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "kept  ") && !strings.HasPrefix(lines[0], "both  ") || !strings.HasSuffix(lines[2], "  finished  (host)") || !strings.Contains(strings.Join(lines, "\n"), "  no result  (join)") {
		t.Fatalf("%q", lines)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("not purged:", err)
	}
	if _, err := os.Stat(strings.TrimSuffix(old, ".moves") + ".json"); err != nil {
		t.Fatal("the result was purged:", err)
	}
	if got := cmdOut(t, "replay", "-profile", "join"); strings.Contains(got, "(join)") || strings.Count(got, "\n") != 2 {
		t.Fatalf("%q", got)
	}
	want := "Room kept, players aaaaaaaa bbbbbbbb\nv2 bbbbbbbb b0 counter=1\nv3 bbbbbbbb b1 counter=2\n2 moves replayed\n"
	if got := cmdOut(t, "replay", "kept"); got != want {
		t.Fatalf("%q", got)
	}
	if got := cmdOut(t, "replay", filepath.Join(root, "join", "games", "kept.moves")); got != want {
		t.Fatalf("%q", got)
	}
	for args, msg := range map[string]string{
		"replay both":               "room both is in profiles host, join: choose one with -profile",
		"replay -profile host kept": `no replay of room kept; "replay" lists them`,
	} {
		if err := run(strings.Fields(args), nil, io.Discard, io.Discard); err == nil || err.Error() != msg {
			t.Fatal(args, err)
		}
	}
	if err := run([]string{"replay", "-dir", ""}, nil, io.Discard, io.Discard); err == nil {
		t.Fatal("an empty -dir listed every profile")
	}

	// A gap: what comes before it is printed, then the error.
	f, err := os.OpenFile(filepath.Join(root, "join", "games", "kept.moves"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"Seq":9,"Peer":"peer-aaaaaaaa","ID":"a","Body":"KzE="}` + "\n")
	f.Close()
	var b strings.Builder
	err = run([]string{"replay", "-profile", "join", "kept"}, nil, &b, io.Discard)
	if err == nil || !strings.HasPrefix(err.Error(), "moves 4 to 8 are missing") || b.String() != strings.TrimSuffix(want, "2 moves replayed\n") {
		t.Fatalf("%q %v", b.String(), err)
	}
}
