package lan

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/kaobei-alabo/you-be-the-ball/game"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// Timing tunes replication. Zero values mean the defaults; the election
// timeout equals Heartbeat.
type Timing struct{ Heartbeat, Lease, Commit time.Duration }

// Options: records go to Dir/games; an empty Dir writes no record.
type Options struct {
	Dir    string
	Timing Timing
}

// Phases a Player has besides those of a started game.
const (
	Lobby  room.Phase = "lobby"
	Closed room.Phase = "closed"
)

type Status struct {
	ID      string
	Phase   room.Phase // Lobby, then room.Playing, Finished or Expired; Closed once left
	Players int
	Online  []string // lobby only: peers seated, host included
	Roster  []string // from the start on
	Version int      // 0 in the lobby, 1 right after the start
	Expires time.Time
	Host    string // peer ID of the current host; "" while none is known
	IsHost  bool
	Paused  bool   // playing, but no host is reachable: moves cannot be made
	Record  string // path of the saved record once written
}

var (
	errClosed     = errors.New("room closed")
	errNotStarted = errors.New("the game has not started")
	ErrNoHost     = errors.New("no host reachable; retry with the same ID")
)

const (
	silence = 10 * time.Second // a lobby guest this quiet loses its seat
	ping    = 2 * time.Second
)

// Player is one person's seat in one room, from the lobby to the end.
type Player struct {
	n       *Node
	g       game.Game
	o       Options
	id      string // room ID
	self    string // replication address of this run
	lobby   *room.Lobby
	changes chan struct{}
	done    chan struct{}
	applied chan struct{} // closed and replaced whenever the state changes

	mu      sync.Mutex
	info    room.LobbyInfo
	guests  map[peer.ID]*link // creator, until the start
	up      *link             // guest: stream to the creator, until the start
	started bool
	state   *room.State
	roster  map[peer.ID]bool
	expires time.Time
	raft    *raft.Raft
	layer   *layer
	obs     *raft.Observer
	obsCh   chan raft.Observation
	closed  bool
	saved   bool
	record  string

	seats     sync.Mutex // orders lobby broadcasts
	proposing sync.Mutex
	barrier   uint64 // term in which a Barrier last succeeded
}

func newPlayer(n *Node, g game.Game, o Options, id string) *Player {
	return &Player{n: n, g: g, o: o, id: id, self: address(n.ID(), id, room.Token()),
		changes: make(chan struct{}, 1), done: make(chan struct{}), applied: make(chan struct{})}
}

func (p *Player) timing() Timing {
	t := p.o.Timing
	if t.Heartbeat == 0 {
		t.Heartbeat = 500 * time.Millisecond
	}
	if t.Lease == 0 {
		t.Lease = 250 * time.Millisecond
	}
	if t.Commit == 0 {
		t.Commit = 10 * time.Millisecond
	}
	return t
}

func (p *Player) signal() {
	select {
	case p.changes <- struct{}{}:
	default:
	}
}

func (p *Player) replica() *raft.Raft {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raft
}

func listen(n *Node) ([]string, error) {
	addrs, err := n.addrs()
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	return out, err
}

// Close leaves the room. A creator in the lobby closes it. A host mid-game
// hands over first; a game still being played is saved as "interrupted".
func (p *Player) Close() error {
	p.stop(true)
	return nil
}

// stop without graceful is a failed start or a lost lobby: nothing to hand over or save.
func (p *Player) stop(graceful bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	r, l, obs, ch, up, guests := p.raft, p.layer, p.obs, p.obsCh, p.up, p.guests
	p.guests = nil
	p.mu.Unlock()
	p.n.remove(p)
	if p.lobby != nil {
		p.lobby.Close()
	}
	for _, g := range guests {
		_ = g.send(reply{Err: errClosed.Error()})
		_ = g.s.Close()
	}
	if up != nil {
		_ = up.s.Close()
	}
	if r != nil {
		if graceful {
			p.handOver(r)
		}
		p.shutdown(r, l, obs, ch)
		if graceful {
			p.save()
		}
	}
	p.signal()
}

func (p *Player) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := Status{ID: p.id, Phase: Lobby, Players: p.info.Players, Online: p.info.Online, Expires: p.info.Expires, Host: p.info.Host, IsHost: p.lobby != nil, Record: p.record}
	if p.state != nil {
		info := p.state.Info()
		s = Status{ID: p.id, Phase: info.Phase, Players: info.Players, Roster: info.Roster, Version: info.Version, Expires: info.Expires, Record: p.record}
		if p.raft != nil {
			if id, ok := hostOf(p.raft); ok {
				s.Host = id.String()
			}
			s.IsHost = p.raft.State() == raft.Leader
		}
		s.Paused = info.Phase == room.Playing && s.Host == ""
	}
	if p.closed {
		s.Phase, s.Host, s.IsHost, s.Paused = Closed, "", false, false
	}
	return s
}

// State is the committed game state; nil in the lobby.
func (p *Player) State() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == nil {
		return nil
	}
	return p.state.View()
}

// Changes is signalled after anything in Status or State changed.
func (p *Player) Changes() <-chan struct{} { return p.changes }

// Submit makes a move and returns its sequence number. It retries with the same
// ID until a host commits it or ctx ends (after 10 s if ctx has no deadline);
// the ID makes a retry count once. It returns once this player's own copy holds
// the move, or at most a second after the host committed it.
func (p *Player) Submit(ctx context.Context, id string, cmd []byte) (int, error) {
	seq, err := p.retry(ctx, func() (reply, error) { return p.try(ctx, id, cmd) })
	if err == nil {
		p.catchUp(seq, time.Second)
	}
	return seq, err
}

// catchUp waits until the local copy reaches version seq, or for at most d.
func (p *Player) catchUp(seq int, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		p.mu.Lock()
		v, ch := p.state.Info().Version, p.applied
		p.mu.Unlock()
		if v >= seq {
			return
		}
		select {
		case <-ch:
		case <-t.C:
			return
		case <-p.done:
			return
		}
	}
}

// changed wakes catchUp; the caller holds p.mu.
func (p *Player) changed() {
	close(p.applied)
	p.applied = make(chan struct{})
}

// Finish ends the game. Only the current host can.
func (p *Player) Finish(ctx context.Context) error {
	_, err := p.retry(ctx, func() (reply, error) {
		r := p.replica()
		if r == nil {
			return reply{}, errNotStarted
		}
		if r.State() != raft.Leader {
			return reply{}, errors.New("only the host can finish the game")
		}
		return p.end(room.KindFinish), nil
	})
	return err
}

func (p *Player) retry(ctx context.Context, f func() (reply, error)) (int, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	for {
		r, err := f()
		switch {
		case err != nil:
			return 0, err
		case r.Err != "":
			return 0, errors.New(r.Err)
		case r.Seq > 0:
			return r.Seq, nil
		}
		select {
		case <-ctx.Done():
			return 0, ErrNoHost
		case <-p.done:
			return 0, errClosed
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (p *Player) try(ctx context.Context, id string, cmd []byte) (reply, error) {
	me := p.n.ID().String()
	p.mu.Lock()
	r, st, closed, expires := p.raft, p.state, p.closed, p.expires
	var phase room.Phase
	seq, done := 0, false
	if st != nil {
		phase = st.Info().Phase
		seq, done = st.Receipt(me, id) // a retry of a move that counted, even after the end
	}
	p.mu.Unlock()
	switch {
	case closed:
		return reply{}, errClosed
	case r == nil:
		return reply{}, errNotStarted
	case done:
		return reply{Seq: seq}, nil
	case phase == room.Expired || !time.Now().Before(expires):
		return reply{}, room.ErrExpired
	case phase != room.Playing:
		return reply{}, room.ErrOver
	case r.State() == raft.Leader:
		return p.decide(me, id, cmd), nil
	}
	to, ok := hostOf(r)
	if !ok || to == p.n.ID() {
		return reply{Retry: true}, nil
	}
	res, err := ask(ctx, p.n, to, request{Op: "submit", Room: p.id, ID: id, Body: cmd})
	if err != nil {
		return reply{Retry: true}, nil
	}
	return res, nil
}
