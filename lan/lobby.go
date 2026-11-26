package lan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/kaobei-alabo/you-be-the-ball/game"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// link is one lobby stream; writes are serialized.
type link struct {
	s     network.Stream
	wmu   sync.Mutex
	seat  *room.Seat
	ready chan struct{}
	gone  chan struct{}
	once  sync.Once
}

func (l *link) send(v any) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	_ = l.s.SetWriteDeadline(time.Now().Add(timeout))
	return send(l.s, v)
}

// Host opens a lobby on n. c.Host is set to n's peer ID.
func Host(n *Node, g game.Game, c room.Config, o Options) (*Player, error) {
	c.Host = n.ID().String()
	l, err := room.NewLobby(c)
	if err != nil {
		return nil, err
	}
	info, err := l.Info()
	if err != nil {
		return nil, err
	}
	p := newPlayer(n, g, o, info.ID)
	p.lobby, p.info, p.guests = l, info, make(map[peer.ID]*link)
	return p, n.add(p)
}

// Join returns once seated in the lobby of the invite. If the game has started,
// a player whose program restarted takes its place back (see rejoin) and Join
// returns once this copy is up to date and playing.
func Join(ctx context.Context, n *Node, g game.Game, inv Invite, o Options) (*Player, error) {
	p := newPlayer(n, g, o, inv.Room)
	if err := n.add(p); err != nil {
		return nil, err
	}
	s, r, err := p.join(ctx, inv)
	if err == nil && r.Started {
		_ = s.Close()
		if err = p.rejoin(ctx, inv.Host.ID); err != nil {
			p.stop(false)
			return nil, err
		}
		return p, nil
	}
	if err == nil && r.Err != "" {
		err = errors.New(r.Err)
	} else if err == nil && r.Lobby == nil {
		err = errors.New("no answer from the room's host")
	}
	if err != nil {
		if s != nil {
			_ = s.Reset()
		}
		n.remove(p)
		return nil, err
	}
	_ = s.SetDeadline(time.Time{})
	p.mu.Lock()
	up := &link{s: s}
	p.info, p.up = *r.Lobby, up
	p.mu.Unlock()
	go p.follow(up, inv.Host.ID)
	return p, nil
}

func (p *Player) join(ctx context.Context, inv Invite) (network.Stream, reply, error) {
	var r reply
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	addrs, err := listen(p.n)
	if err != nil {
		return nil, r, err
	}
	if inv.Host.ID == p.n.ID() {
		return nil, r, errors.New("this is your own invite; ask another player for a fresh one")
	}
	if err = p.n.h.Connect(ctx, inv.Host); err != nil {
		return nil, r, fmt.Errorf("reach the player who gave the invite (after the start, any player still in the game can give a fresh one): %w", err)
	}
	s, err := p.n.h.NewStream(ctx, inv.Host.ID, lobbyProto)
	if err != nil && strings.Contains(err.Error(), "protocols not supported") {
		err = errors.New("the room's host runs an older you-be-the-ball; both must upgrade")
	}
	if err != nil {
		return nil, r, err
	}
	dl, _ := ctx.Deadline()
	if err = s.SetDeadline(dl); err == nil {
		err = send(s, request{Op: "join", Room: inv.Room, Secret: inv.Secret, Addrs: addrs, Node: p.self})
	}
	if err == nil {
		err = recv(s, &r)
	}
	return s, r, err
}

// follow is the guest's side of the lobby stream: seat changes, then the
// Setup. It pings so the host keeps the seat.
func (p *Player) follow(up *link, host peer.ID) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		t := time.NewTicker(ping)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if up.send(request{Op: "ping"}) != nil {
					return
				}
			}
		}
	}()
	for {
		var r reply
		if err := recv(up.s, &r); err != nil {
			if p.replica() == nil {
				p.stop(false) // the room closed before the start
			}
			return
		}
		switch {
		case r.Err != "":
			p.stop(false)
			return
		case r.Setup != nil:
			if s := *r.Setup; p.replica() != nil || s.ID != p.id || len(s.Roster) == 0 || s.Roster[0].Peer != host.String() || p.begin(s, false) != nil {
				p.stop(false)
				return
			}
			_ = up.send(request{Op: "ready"})
			// The creator bootstraps; without a word from it the start failed.
			time.AfterFunc(silence, func() {
				if r := p.replica(); r != nil && r.LastIndex() == 0 {
					p.stop(false)
				}
			})
		case r.Lobby != nil:
			p.mu.Lock()
			p.info = *r.Lobby
			p.mu.Unlock()
			p.signal()
		}
	}
}

// admit serves one guest's lobby stream on the creator's side.
func (p *Player) admit(s network.Stream, from peer.ID, q request) {
	l := &link{s: s, ready: make(chan struct{}), gone: make(chan struct{})}
	defer close(l.gone)
	if err := p.seat(l, from, q); err != nil {
		_ = l.send(reply{Err: err.Error()})
		_ = s.Close()
		return
	}
	p.broadcast()
	for {
		var m request
		if s.SetReadDeadline(time.Now().Add(silence)) != nil || recv(s, &m) != nil {
			break
		}
		if m.Op == "ready" {
			l.once.Do(func() { close(l.ready) })
		}
	}
	_ = s.Reset()
	p.mu.Lock()
	l.seat.Leave()
	if p.guests[from] == l {
		delete(p.guests, from)
	}
	p.mu.Unlock()
	p.broadcast()
}

// checkRun vets what a caller says about its run: the replication address must
// be its own, in this room, and its listen addresses few and valid.
func (p *Player) checkRun(from peer.ID, q request) error {
	prefix := from.String() + "/" + p.id + "/"
	if _, _, err := parseAddress(q.Node); err != nil || !strings.HasPrefix(q.Node, prefix) || len(q.Node) > 512 {
		return errors.New("invalid replication address")
	}
	if len(q.Addrs) > 16 {
		return errors.New("too many addresses")
	}
	for _, a := range q.Addrs {
		if _, err := ma.NewMultiaddr(a); err != nil {
			return err
		}
	}
	return nil
}

func (p *Player) seat(l *link, from peer.ID, q request) error {
	if err := p.checkRun(from, q); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.lobby == nil:
		return room.ErrInvite // a guest's node holds no lobby
	case p.closed:
		return errClosed
	case p.started:
		return room.ErrStarted
	}
	seat, err := p.lobby.Join(room.Member{Peer: from.String(), Addrs: q.Addrs, Node: q.Node}, q.Room, q.Secret)
	if err != nil {
		return err
	}
	if old := p.guests[from]; old != nil {
		_ = old.s.Reset() // superseded by this connection
	}
	l.seat = seat
	p.guests[from] = l
	return nil
}

// broadcast sends the seats to every guest. Serialized so no guest sees an
// older list after a newer one.
func (p *Player) broadcast() {
	p.seats.Lock()
	defer p.seats.Unlock()
	info, err := p.lobby.Info()
	if err != nil {
		return
	}
	p.mu.Lock()
	p.info = info
	links := make([]*link, 0, len(p.guests))
	for _, l := range p.guests {
		links = append(links, l)
	}
	p.mu.Unlock()
	for _, l := range links {
		_ = l.send(reply{Lobby: &info})
	}
	p.signal()
}

// Invite is the creator's invite in the lobby. After the start any player's
// invite lets a player whose program restarted come back; it carries no
// secret, since only the roster is admitted then.
func (p *Player) Invite() (Invite, error) {
	p.mu.Lock()
	closed, started := p.closed, p.state != nil
	p.mu.Unlock()
	switch {
	case closed:
		return Invite{}, errClosed
	case started:
		addrs, err := p.n.addrs()
		return Invite{Room: p.id, Host: peer.AddrInfo{ID: p.n.ID(), Addrs: addrs}}, err
	case p.lobby == nil:
		return Invite{}, errors.New("only the room's creator has its invite")
	}
	id, secret, err := p.lobby.Invite()
	if err != nil {
		return Invite{}, err
	}
	addrs, err := p.n.addrs()
	return Invite{Room: id, Host: peer.AddrInfo{ID: p.n.ID(), Addrs: addrs}, Secret: secret}, err
}

// Start begins the game once every seat is taken. Only the creator can start.
func (p *Player) Start(ctx context.Context) error {
	if p.lobby == nil {
		return errors.New("only the room's creator can start it")
	}
	addrs, err := listen(p.n)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errClosed
	}
	setup, err := p.lobby.Start(room.Member{Peer: p.n.ID().String(), Addrs: addrs, Node: p.self})
	var links []*link
	if err == nil {
		p.started = true
		for _, m := range setup.Roster[1:] {
			if id, err := peer.Decode(m.Peer); err == nil && p.guests[id] != nil {
				links = append(links, p.guests[id])
			}
		}
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	fail := func(err error) error {
		for _, l := range links {
			_ = l.send(reply{Err: "the game could not start: " + err.Error()})
		}
		p.stop(false)
		return err
	}
	if len(links) != len(setup.Roster)-1 {
		return fail(errors.New("a player left"))
	}
	ctx, cancel := context.WithTimeout(ctx, silence)
	defer cancel()
	for _, l := range links {
		if err := l.send(reply{Setup: &setup}); err != nil {
			return fail(err)
		}
	}
	for _, l := range links {
		select {
		case <-l.ready:
		case <-l.gone:
			return fail(errors.New("a player left"))
		case <-ctx.Done():
			return fail(errors.New("not every player got ready"))
		}
	}
	if err := p.begin(setup, false); err != nil {
		return fail(err)
	}
	r := p.replica()
	if err := wait(r.BootstrapCluster(raft.Configuration{Servers: servers(setup)}), timeout); err != nil {
		return fail(err)
	}
	for r.State() != raft.Leader {
		select {
		case <-ctx.Done():
			return fail(errors.New("no host was chosen"))
		case <-time.After(5 * time.Millisecond):
		}
	}
	p.proposing.Lock()
	term := r.CurrentTerm()
	err = wait(r.Barrier(timeout), timeout)
	if err == nil {
		p.barrier = term
	}
	p.proposing.Unlock()
	if err != nil {
		return fail(err)
	}
	p.mu.Lock()
	p.guests = nil
	p.mu.Unlock()
	for _, l := range links {
		_ = l.s.Close()
	}
	return nil
}
