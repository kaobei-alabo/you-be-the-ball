package lan

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// playDirs is play, also returning each player's profile directory.
func playDirs(t *testing.T, c room.Config) ([]*Player, []string) {
	t.Helper()
	dirs := make([]string, c.Players)
	nodes := make([]*Node, c.Players)
	for i := range nodes {
		dirs[i] = t.TempDir()
		nodes[i] = newNode(t, dirs[i])
	}
	return playOn(t, nodes, c, fast), dirs
}

// restart runs a crashed player's program again: a new Node on the same
// profile, so with the same identity, joining with inv.
func restart(t *testing.T, dir string, inv Invite) (*Player, error) {
	t.Helper()
	return join(t, newNode(t, dir), inv, Options{Dir: t.TempDir(), Timing: fast})
}

// original is the creator's invite as the lobby gave it (public: no secret).
func original(t *testing.T, p *Player) Invite {
	t.Helper()
	addrs, err := p.n.addrs()
	if err != nil {
		t.Fatal(err)
	}
	return Invite{Room: p.id, Host: peer.AddrInfo{ID: p.n.ID(), Addrs: addrs}}
}

func inviteOf(t *testing.T, p *Player) Invite {
	t.Helper()
	inv, err := p.Invite()
	if err != nil || inv.Secret != "" {
		t.Fatal("invite after the start:", inv, err)
	}
	return inv
}

func configOf(p *Player) ([]raft.Server, error) {
	f := p.replica().GetConfiguration()
	if err := wait(f, timeout); err != nil {
		return nil, err
	}
	return f.Configuration().Servers, nil
}

// suffrage is how p's raft configuration counts the current run of x; it fails
// the test if an older run of x is still in it.
func suffrage(t *testing.T, p, x *Player) raft.ServerSuffrage {
	t.Helper()
	servers, err := configOf(p)
	if err != nil {
		t.Fatal(err)
	}
	s := raft.ServerSuffrage(-1)
	for _, v := range servers {
		switch {
		case string(v.ID) == x.self:
			s = v.Suffrage
		case strings.HasPrefix(string(v.ID), id(x)+"/"):
			t.Fatal("an old run is still in the configuration:", v.ID)
		}
	}
	return s
}

func TestRestartedPlayerRejoinsAndCountsAgain(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inv := original(t, ps[0])
	for i, p := range ps {
		inc(t, p, fmt.Sprint("m", i))
	}
	crash(ps[2])
	inc(t, ps[1], "while-away")
	c, err := restart(t, dirs[2], inv)
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Status(); s.Phase != room.Playing || s.Version != ps[0].Status().Version || string(c.State()) != "4" {
		t.Fatal("rejoined at", s, string(c.State()))
	}
	if s := suffrage(t, ps[0], c); s != raft.Voter {
		t.Fatal("the rejoined player is", s)
	}
	inc(t, c, "back")
	live := []*Player{ps[0], ps[1], c}
	converged(t, live, "5")

	// Only a majority with the rejoined player can go on without the host.
	crash(ps[0])
	leader(t, ps[1], c)
	inc(t, ps[1], "x")
	inc(t, c, "y")
	converged(t, []*Player{ps[1], c}, "7")
}

func TestRestartedHostRejoinsAsAPlayer(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inc(t, ps[1], "a")
	crash(ps[0])
	leader(t, ps[1:]...)
	inc(t, ps[2], "b")
	n := newNode(t, dirs[0])
	if _, err := join(t, n, original(t, ps[0]), Options{Timing: fast}); err == nil || err.Error() != "this is your own invite; ask another player for a fresh one" {
		t.Fatal("rejoin with its own invite:", err)
	}
	n.Close()
	a, err := restart(t, dirs[0], inviteOf(t, ps[1]))
	if err != nil {
		t.Fatal(err)
	}
	if a.Status().IsHost || string(a.State()) != "2" {
		t.Fatal("rejoined host", a.Status(), string(a.State()))
	}
	live := []*Player{a, ps[1], ps[2]}
	if seq := inc(t, a, "c"); seq != 4 {
		t.Fatal("seq", seq)
	}
	converged(t, live, "3")
}

func TestRejoinThroughAPlayerWhoIsNotTheHost(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inc(t, ps[2], "a")
	crash(ps[2])
	if leader(t, ps[:2]...) != ps[0] {
		t.Fatal("the host changed")
	}
	c, err := restart(t, dirs[2], inviteOf(t, ps[1])) // ps[1] redirects to ps[0]
	if err != nil {
		t.Fatal(err)
	}
	if s := suffrage(t, ps[0], c); s != raft.Voter {
		t.Fatal("the rejoined player is", s)
	}
	inc(t, c, "b")
	converged(t, []*Player{ps[0], ps[1], c}, "2")
}

func TestTwoPlayerGuestRejoins(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 2})
	h := ps[0]
	inv := original(t, h)
	inc(t, ps[1], "g1")
	crash(ps[1])
	stop, done := make(chan struct{}), make(chan int)
	go func() { // the host plays on throughout
		n := 0
		defer func() { done <- n }()
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := h.Submit(ctx, fmt.Sprint("h", n), []byte("inc"))
			cancel()
			if err != nil || h.Status().Paused {
				t.Error("the host could not play on:", err, h.Status())
				return
			}
			n++
		}
	}()
	time.Sleep(100 * time.Millisecond) // a few host moves while the guest is away
	g, err := restart(t, dirs[1], inv)
	time.Sleep(100 * time.Millisecond)
	close(stop)
	n := <-done
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("the host made no move")
	}
	converged(t, []*Player{h, g}, fmt.Sprint(n+1))
	if s := suffrage(t, h, g); s != raft.Nonvoter {
		t.Fatal("the guest is", s)
	}
	inc(t, g, "g2")

	h.n.SetOffline(true)
	waitFor(t, "the guest pauses", func() bool { return g.Status().Paused })
	fails(t, g, "g3", 1500*time.Millisecond)
	if s := g.Status(); s.IsHost || !s.Paused {
		t.Fatal("the guest took over", s)
	}

	// Nobody can replace the creator: it was the only voter.
	crash(h)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := Join(ctx, newNode(t, dirs[0]), h.g, inviteOf(t, g), Options{Timing: fast}); err == nil || err.Error() != "the creator of a two-player game cannot rejoin it" {
		t.Fatal("the creator of a two-player game rejoined:", err)
	}
	if s := g.Status(); s.IsHost || !s.Paused {
		t.Fatal("the guest took over", s)
	}
}

func TestOnlyThePlayersThemselvesCanRejoin(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	inc(t, ps[2], "a")
	stranger := newNode(t, t.TempDir())
	if _, err := join(t, stranger, inviteOf(t, ps[1]), Options{Timing: fast}); err == nil || err.Error() != "not a player in this game" {
		t.Fatal("a stranger rejoined:", err)
	}
	before, err := configOf(ps[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, to := range []*Player{ps[0], ps[2]} { // the host, and a player that redirects
		q := request{Op: "rejoin", Room: ps[0].id, Node: address(ps[2].n.ID(), ps[0].id, "x")}
		if r, err := ask(ctx, ps[1].n, to.n.ID(), q); err != nil || r.Err != "invalid replication address" {
			t.Fatal("a rejoin in another player's name:", r, err)
		}
	}
	if after, err := configOf(ps[0]); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("the configuration changed:", before, after, err)
	}
	inc(t, ps[2], "b")
	converged(t, ps, "2")
}

func TestRejoinNeedsAHost(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inc(t, ps[1], "a")
	inv := inviteOf(t, ps[0])
	crash(ps[1])
	crash(ps[2])
	waitFor(t, "no host", func() bool { s := ps[0].Status(); return !s.IsHost && s.Host == "" })
	n := newNode(t, dirs[2])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	begin := time.Now()
	_, err := Join(ctx, n, ps[0].g, inv, Options{Timing: fast})
	if err == nil || !strings.Contains(err.Error(), "no host reachable") {
		t.Fatal("rejoined without a host:", err)
	}
	if d := time.Since(begin); d > 4*time.Second {
		t.Fatal("Join took", d)
	}
	if n.room(inv.Room) != nil || slices.ContainsFunc(n.h.Mux().Protocols(), func(p protocol.ID) bool { return strings.HasPrefix(string(p), raftProto) }) {
		t.Fatal("a failed rejoin left its copy running")
	}
}

func TestRejoinDuringHostChange(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 5})
	for i, p := range ps {
		inc(t, p, fmt.Sprint("m", i))
	}
	h, old := ps[0], ps[4]
	crash(old)
	// The host serves the rejoin's first call (the old run removed, the new one
	// added) and then crashes, before it can promote the new run.
	var gone atomic.Bool
	crashed := make(chan struct{})
	h.n.h.SetStreamHandler(lobbyProto, func(s network.Stream) {
		if gone.Load() {
			_ = s.Reset()
			return
		}
		h.n.serve(s)
		servers, _ := configOf(h)
		if slices.ContainsFunc(servers, func(v raft.Server) bool {
			return strings.HasPrefix(string(v.ID), id(old)+"/") && string(v.ID) != old.self
		}) && gone.CompareAndSwap(false, true) {
			go func() { crash(h); close(crashed) }()
		}
	})
	e, err := restart(t, dirs[4], original(t, h))
	if err != nil {
		t.Fatal(err)
	}
	if !gone.Load() {
		t.Fatal("the host was not crashed during the rejoin")
	}
	<-crashed
	live := append(slices.Clone(ps[1:4]), e)
	nh := leader(t, live...)
	if s := suffrage(t, nh, e); s != raft.Voter {
		t.Fatal("the rejoined player is", s)
	}
	inc(t, e, "e")
	converged(t, live, "6")
}

// answer replaces p's lobby handler: a join learns the game has started, a
// rejoin gets f's reply (p answering as a member would when f returns nil).
func answer(p *Player, f func(from peer.ID, q request) *reply) {
	p.n.h.SetStreamHandler(lobbyProto, func(s network.Stream) {
		defer s.Close()
		var q request
		if recv(s, &q) != nil {
			return
		}
		from := s.Conn().RemotePeer()
		r := reply{Started: true}
		if q.Op == "rejoin" {
			if f := f(from, q); f != nil {
				r = *f
			} else {
				r = p.serveRejoin(from, q)
			}
		}
		_ = send(s, r)
	})
}

// Only the host's Setup builds a rejoining copy: a member answering as if it
// were the host cannot plant another one.
func TestRejoinRefusesAForgedSetup(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inc(t, ps[2], "a")
	crash(ps[2])
	ps[0].mu.Lock()
	forged := ps[0].state.Setup()
	ps[0].mu.Unlock()
	forged.Expires = time.Now().Add(3 * time.Second)
	var n atomic.Int32
	answer(ps[1], func(peer.ID, request) *reply {
		if n.Add(1) == 1 {
			return &reply{Setup: &forged, Seq: 1} // then redirect to the host
		}
		return nil
	})
	node := newNode(t, dirs[2])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := Join(ctx, node, ps[0].g, inviteOf(t, ps[1]), Options{Timing: fast}); err == nil || err.Error() != "invalid game setup" {
		t.Fatal("rejoined with a forged setup:", err)
	}
	node.Close()
	c, err := restart(t, dirs[2], original(t, ps[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Status().Expires.Equal(ps[0].Status().Expires) {
		t.Fatal("expiry", c.Status().Expires, ps[0].Status().Expires)
	}
	inc(t, c, "b")
	converged(t, []*Player{ps[0], ps[1], c}, "2")
}

// A member that cannot help does not end a rejoin: here the room is gone from
// the invite's node at first, as on a member that is itself restarting.
func TestRejoinAsksAgainWhenAMemberCannotHelp(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	crash(ps[2])
	var n atomic.Int32
	answer(ps[1], func(peer.ID, request) *reply {
		if n.Add(1) == 1 {
			return &reply{Err: errNoRoom.Error()}
		}
		return nil
	})
	c, err := restart(t, dirs[2], inviteOf(t, ps[1]))
	if err != nil {
		t.Fatal(err)
	}
	inc(t, c, "a")
	converged(t, []*Player{ps[0], ps[1], c}, "1")

	p := newPlayer(newNode(t, t.TempDir()), ps[0].g, Options{}, ps[0].id) // a copy not yet built
	if r := p.serveRejoin(ps[2].n.ID(), request{}); !r.Retry {
		t.Fatal("a member without a copy answered", r)
	}
}

// Two players restart one after the other, each on a new port, then the host
// crashes: the two must still reach each other to choose a host.
func TestTwoRestartedPlayersReachEachOther(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inv := original(t, ps[0])
	crash(ps[2])
	c, err := restart(t, dirs[2], inv)
	if err != nil {
		t.Fatal(err)
	}
	crash(ps[1])
	b, err := restart(t, dirs[1], inv)
	if err != nil {
		t.Fatal(err)
	}
	inc(t, b, "a")
	crash(ps[0])
	leader(t, b, c)
	inc(t, b, "b")
	inc(t, c, "c")
	converged(t, []*Player{b, c}, "3")
}
