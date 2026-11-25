package lan

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// A player whose program restarted has lost its copy and comes back as a new
// raft server: answering at its old replication address with an empty memory
// would let it be counted as the run that held that address's votes and log.
// The host removes the old run, adds the new one as a nonvoter and, once it
// has caught up, makes it a voter again if its seat votes.

// serveRejoin answers a rejoining player. A member stores its addresses and
// says where the host is; the host does the next steps of the replacement and
// alone sends the Setup, so another member cannot plant a different one.
func (p *Player) serveRejoin(from peer.ID, q request) reply {
	p.mu.Lock()
	r, member := p.raft, p.roster[from]
	var setup room.Setup
	if p.state != nil {
		setup = p.state.Setup()
	}
	p.mu.Unlock()
	switch {
	case r == nil:
		return reply{Retry: true} // this copy is still being built
	case !member:
		return reply{Err: "not a player in this game"}
	case setup.Players == 2 && setup.Roster[0].Peer == from.String():
		return reply{Err: "the creator of a two-player game cannot rejoin it"}
	}
	if err := p.checkRun(from, q); err != nil {
		return reply{Err: err.Error()}
	}
	// Any member may host later, and must then reach the new run.
	for _, a := range q.Addrs {
		if a, err := ma.NewMultiaddr(a); err == nil {
			p.n.h.Peerstore().AddAddr(from, a, peerstore.PermanentAddrTTL)
		}
	}
	switch to, ok := hostOf(r); {
	case r.State() == raft.Leader:
		p.rejoining.Lock()
		res := p.replace(r, from, q, setup)
		p.rejoining.Unlock()
		if !res.Retry {
			res.Setup, res.Peers = &setup, p.addrsOf(others(setup, from))
		}
		return res
	case ok && to != p.n.ID() && to != from: // not the caller's own old run
		return reply{Redirect: to.String(), Peers: p.addrsOf([]peer.ID{to})}
	}
	return reply{Retry: true}
}

// addrsOf is where this node reaches the given members now: the Setup still
// lists where each one listened at the start, before any of them restarted.
func (p *Player) addrsOf(ids []peer.ID) map[string][]string {
	out := make(map[string][]string)
	for _, id := range ids {
		for _, a := range p.n.h.Peerstore().Addrs(id) {
			out[id.String()] = append(out[id.String()], a.String())
		}
	}
	return out
}

// learn stores addresses a member sent. Wrong ones only fail to dial: a
// connection proves the peer ID.
func (p *Player) learn(addrs map[string][]string) {
	for s, as := range addrs {
		id, err := peer.Decode(s)
		if err != nil || id == p.n.ID() {
			continue
		}
		for _, a := range as {
			if a, err := ma.NewMultiaddr(a); err == nil {
				p.n.h.Peerstore().AddAddr(id, a, peerstore.PermanentAddrTTL)
			}
		}
	}
}

// replace is the host's part. Every step is idempotent, so a caller repeats
// the call until Done, even across a host change.
func (p *Player) replace(r *raft.Raft, from peer.ID, q request, setup room.Setup) reply {
	f := r.GetConfiguration()
	if wait(f, timeout) != nil {
		return reply{Retry: true}
	}
	prefix := from.String() + "/" + p.id + "/"
	suffrage, known := raft.Nonvoter, false
	for _, s := range f.Configuration().Servers {
		switch id := string(s.ID); {
		case id == q.Node:
			suffrage, known = s.Suffrage, true
		case strings.HasPrefix(id, prefix) && id != p.self: // an older run, never this host
			if wait(r.RemoveServer(s.ID, 0, timeout), timeout) != nil {
				return reply{Retry: true}
			}
		}
	}
	if !known && wait(r.AddNonvoter(raft.ServerID(q.Node), raft.ServerAddress(q.Node), 0, timeout), timeout) != nil {
		return reply{Retry: true}
	}
	version := p.version()
	voter := slices.ContainsFunc(setup.Roster, func(m room.Member) bool { return m.Peer == from.String() && m.Voter })
	// Promote only once caught up: a lagging voter would slow every commit.
	if voter && suffrage == raft.Nonvoter && q.Seq >= version {
		if wait(r.AddVoter(raft.ServerID(q.Node), raft.ServerAddress(q.Node), 0, timeout), timeout) != nil {
			return reply{Retry: true}
		}
		suffrage = raft.Voter
	}
	return reply{Seq: version, Done: voter && suffrage == raft.Voter || !voter && q.Seq >= version}
}

// rejoin is the caller's part: ask a member, follow it to the host, and repeat
// with the version this copy has reached until the host is done. It gives up
// when ctx ends (after 30 s if ctx has no deadline).
func (p *Player) rejoin(ctx context.Context, to peer.ID) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	addrs, err := listen(p.n)
	if err != nil {
		return err
	}
	peers := []peer.ID{to} // whom to ask when the last one could not help
	var setup *room.Setup  // the host's, from its first answer
	for i := 0; ; {
		r, err := ask(ctx, p.n, to, request{Op: "rejoin", Room: p.id, Node: p.self, Addrs: addrs, Seq: p.version()})
		if err == nil {
			p.learn(r.Peers)
		}
		switch {
		case err != nil:
		case r.Err == errNoRoom.Error():
			err = errNoRoom // that member's copy is gone or not there yet: ask another
		case r.Err != "":
			return errors.New(r.Err)
		case r.Setup == nil || r.Retry || r.Redirect != "": // not the host's answer
		case setup == nil:
			if err := p.adopt(*r.Setup, to); err != nil {
				return err
			}
			setup = r.Setup
			peers = append(peers, others(*setup, p.n.ID())...)
		case !reflect.DeepEqual(r.Setup, setup):
			return errInvalidSetup // one of the two answers was not the host's
		}
		next, bad := peer.Decode(r.Redirect)
		switch {
		case err != nil || r.Retry:
		case r.Done:
			// Only from the host this copy itself follows: a member posing as the
			// host is not that, and its answer says nothing about this copy.
			if rep := p.replica(); rep != nil {
				if h, ok := hostOf(rep); ok && h == to {
					return nil
				}
			}
		case r.Redirect != "":
			if bad == nil && next != p.n.ID() && next != to {
				to = next
				continue
			}
		case p.replica() != nil:
			p.catchUp(r.Seq, time.Second) // the host answered: catch up, then ask again
			continue
		}
		i++
		to = peers[i%len(peers)]
		select {
		case <-ctx.Done():
			return errors.New("could not rejoin the game: no host reachable; most of the other players must be online")
		case <-p.done:
			return errClosed
		case <-time.After(100 * time.Millisecond):
		}
	}
}

var errInvalidSetup = errors.New("invalid game setup")

// adopt builds this copy from the host's Setup and tells every other member
// this run's addresses: whoever hosts next must reach it.
func (p *Player) adopt(s room.Setup, host peer.ID) error {
	if s.ID != p.id {
		return errInvalidSetup
	}
	if err := p.begin(s, true); err != nil {
		return err
	}
	q := request{Op: "rejoin", Room: p.id, Node: p.self}
	q.Addrs, _ = listen(p.n)
	for _, id := range others(s, p.n.ID()) {
		if id != host {
			go func() { _, _ = ask(context.Background(), p.n, id, q) }()
		}
	}
	return nil
}

func others(s room.Setup, me peer.ID) []peer.ID {
	var out []peer.ID
	for _, m := range s.Roster {
		if id, err := peer.Decode(m.Peer); err == nil && id != me {
			out = append(out, id)
		}
	}
	return out
}
