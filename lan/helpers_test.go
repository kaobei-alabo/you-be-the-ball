package lan

import (
	"context"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game"
	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// theGame is what the helpers play; a test may swap it.
var theGame game.Game = counter.Game{}

// keepMoves: the players of play keep their moves, as the CLI's do by default.
var keepMoves = true

var fast = Timing{Heartbeat: 100 * time.Millisecond, Lease: 50 * time.Millisecond, Commit: 2 * time.Millisecond}

func newNode(t *testing.T, dir string) *Node {
	t.Helper()
	n, err := New(dir, "/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func hostRoom(t *testing.T, c room.Config, o Options) *Player {
	t.Helper()
	p, err := Host(newNode(t, t.TempDir()), theGame, c, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func join(t *testing.T, n *Node, inv Invite, o Options) (*Player, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Join(ctx, n, theGame, inv, o)
	if err == nil {
		t.Cleanup(func() { p.Close() })
	}
	return p, err
}

// play starts a game of c.Players; ps[0] is the creator. Every player records
// to its own Dir.
func play(t *testing.T, c room.Config, tm Timing) []*Player {
	t.Helper()
	nodes := make([]*Node, c.Players)
	for i := range nodes {
		nodes[i] = newNode(t, t.TempDir())
	}
	return playOn(t, nodes, c, tm)
}

func playOn(t *testing.T, nodes []*Node, c room.Config, tm Timing) []*Player {
	t.Helper()
	return playGames(t, nodes, c, tm, func(int) game.Game { return theGame })
}

// playGames is playOn with player i playing games(i).
func playGames(t *testing.T, nodes []*Node, c room.Config, tm Timing, games func(int) game.Game) []*Player {
	t.Helper()
	h, err := Host(nodes[0], games(0), c, Options{Dir: t.TempDir(), KeepMoves: keepMoves, Timing: tm})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	ps := []*Player{h}
	inv, err := h.Invite()
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range nodes[1:] {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p, err := Join(ctx, n, games(i+1), inv, Options{Dir: t.TempDir(), KeepMoves: keepMoves, Timing: tm})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.Close() })
		ps = append(ps, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := ps[0].Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "every player sees the start", func() bool {
		for _, p := range ps {
			if p.Status().Version != 1 || p.Status().Host != ps[0].n.ID().String() {
				return false
			}
		}
		return true
	})
	return ps
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(15 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting until %s", what)
		}
	}
}

// leader waits until exactly one of ps is host and all of ps agree on it.
func leader(t *testing.T, ps ...*Player) *Player {
	t.Helper()
	var h *Player
	waitFor(t, "one host", func() bool {
		h = nil
		for _, p := range ps {
			if p.Status().IsHost {
				if h != nil {
					return false
				}
				h = p
			}
		}
		if h == nil {
			return false
		}
		for _, p := range ps {
			if p.Status().Host != h.n.ID().String() {
				return false
			}
		}
		return true
	})
	return h
}

func without(ps []*Player, x *Player) []*Player {
	var out []*Player
	for _, p := range ps {
		if p != x {
			out = append(out, p)
		}
	}
	return out
}

func inc(t *testing.T, p *Player, id string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seq, err := p.Submit(ctx, id, []byte("inc"))
	if err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	return seq
}

// converged waits until every player holds the same state and version.
func converged(t *testing.T, ps []*Player, want string) {
	t.Helper()
	waitFor(t, "every player holds "+want, func() bool {
		v := ps[0].Status().Version
		for _, p := range ps {
			if string(p.State()) != want || p.Status().Version != v {
				return false
			}
		}
		return true
	})
}

// crash kills a player as a dying process would: no handover, no record.
func crash(p *Player) {
	p.n.Close()
	p.stop(false)
}

// cut severs the link between a and b, or heals it.
func cut(a, b *Player, on bool) {
	a.n.block(b.n.ID(), on)
	b.n.block(a.n.ID(), on)
}

func id(p *Player) string { return p.n.ID().String() }
