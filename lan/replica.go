package lan

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

var errTimeout = errors.New("timed out")

// wait bounds a raft future: Error can block forever on a node shutting down.
func wait(f raft.Future, d time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return errTimeout
	}
}

func servers(s room.Setup) []raft.Server {
	out := make([]raft.Server, len(s.Roster))
	for i, m := range s.Roster {
		out[i] = raft.Server{Suffrage: raft.Nonvoter, ID: raft.ServerID(m.Node), Address: raft.ServerAddress(m.Node)}
		if m.Voter {
			out[i].Suffrage = raft.Voter
		}
	}
	return out
}

// begin builds this member's copy of the game and its raft node, with empty
// stores and no configuration: only the creator bootstraps the cluster.
func (p *Player) begin(setup room.Setup) error {
	st, err := room.NewState(setup, p.g)
	if err != nil {
		return err
	}
	roster := make(map[peer.ID]bool)
	mine := false
	for _, m := range setup.Roster {
		id, err := peer.Decode(m.Peer)
		if err != nil {
			return err
		}
		roster[id] = true
		if id == p.n.ID() {
			mine = m.Node == p.self
			continue
		}
		for _, a := range m.Addrs {
			if a, err := ma.NewMultiaddr(a); err == nil {
				p.n.h.Peerstore().AddAddr(id, a, peerstore.PermanentAddrTTL)
			}
		}
	}
	if !mine {
		return errors.New("this player is not on the roster")
	}
	l, err := newLayer(p.n, p.self, roster)
	if err != nil {
		return err
	}
	t := p.timing()
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(p.self)
	c.HeartbeatTimeout, c.ElectionTimeout = t.Heartbeat, t.Heartbeat
	c.LeaderLeaseTimeout, c.CommitTimeout = t.Lease, t.Commit
	c.Logger = hclog.NewNullLogger()
	trans := raft.NewNetworkTransportWithLogger(l, 3, 2*time.Second, hclog.NewNullLogger())
	p.mu.Lock()
	p.state, p.roster, p.expires = st, roster, setup.Expires
	p.mu.Unlock()
	s := newStore()
	r, err := raft.NewRaft(c, (*fsm)(p), s, s, raft.NewInmemSnapshotStore(), trans)
	if err != nil {
		l.Close()
		return err
	}
	ch := make(chan raft.Observation, 64)
	obs := raft.NewObserver(ch, false, func(o *raft.Observation) bool {
		_, leader := o.Data.(raft.LeaderObservation)
		_, state := o.Data.(raft.RaftState)
		return leader || state
	})
	r.RegisterObserver(obs)
	p.mu.Lock()
	closed := p.closed
	p.raft, p.layer, p.obs, p.obsCh = r, l, obs, ch
	p.mu.Unlock()
	if closed {
		p.shutdown(r, l, obs, ch)
		return errClosed
	}
	go func() {
		for range ch {
			p.signal()
		}
	}()
	go p.expire(setup.Expires)
	p.signal()
	return nil
}

func (p *Player) shutdown(r *raft.Raft, l *layer, obs *raft.Observer, ch chan raft.Observation) {
	l.Close() // first: it ends dials that would hold up the shutdown
	_ = wait(r.Shutdown(), timeout)
	// Deregistering takes raft's observer lock, so no send can race the close.
	r.DeregisterObserver(obs)
	close(ch)
}

// fsm is the raft.FSM view of a Player.
type fsm Player

// Apply reads no clock: every member must reach the same result.
func (f *fsm) Apply(l *raft.Log) any {
	p := (*Player)(f)
	var e room.Entry
	if err := json.Unmarshal(l.Data, &e); err != nil {
		return room.Result{Err: "invalid entry"}
	}
	p.mu.Lock()
	res := p.state.Apply(e)
	over := p.state.Info().Phase != room.Playing
	p.changed()
	p.mu.Unlock()
	if over {
		p.save()
	}
	p.signal()
	return res
}

type snapshot []byte

func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (snapshot) Release() {}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	p := (*Player)(f)
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := p.state.Snapshot()
	return snapshot(b), err
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	p := (*Player)(f)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	st, err := room.Restore(b, p.g)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.state = st
	p.changed()
	p.mu.Unlock()
	if st.Info().Phase != room.Playing {
		p.save()
	}
	p.signal()
	return nil
}

// save writes the record once: when the game ends, or "interrupted" on Close.
func (p *Player) save() {
	p.mu.Lock()
	if p.saved || p.state == nil || p.o.Dir == "" {
		p.mu.Unlock()
		return
	}
	p.saved = true
	rec := p.state.Record()
	p.mu.Unlock()
	if file, err := room.WriteRecord(p.o.Dir, rec); err == nil {
		p.mu.Lock()
		p.record = file
		p.mu.Unlock()
		p.signal()
	}
}

// notHost tells a caller where the host is, if this node knows.
func (p *Player) notHost(r *raft.Raft) reply {
	if id, ok := hostOf(r); ok && id != p.n.ID() {
		return reply{Redirect: id.String()}
	}
	return reply{Retry: true}
}

// propose is the host's only way to change the game. A failed Apply is not a
// "no": the entry may still commit, so the caller retries with the same ID and
// the state's receipts answer the repeat.
func (p *Player) propose(build func(*room.State) (room.Entry, *room.Result, error)) reply {
	p.proposing.Lock()
	defer p.proposing.Unlock()
	r := p.replica()
	if r == nil {
		return reply{Err: errNotStarted.Error()}
	}
	for range 5 {
		if r.State() != raft.Leader {
			return p.notHost(r)
		}
		// Apply everything committed before this term before deciding anything.
		if term := r.CurrentTerm(); p.barrier != term {
			if wait(r.Barrier(timeout), timeout) != nil {
				return reply{Retry: true}
			}
			p.barrier = term
		}
		p.mu.Lock()
		e, done, err := build(p.state)
		p.mu.Unlock()
		if err != nil {
			return reply{Err: err.Error()}
		}
		if done != nil {
			return reply{Seq: done.Seq}
		}
		data, err := json.Marshal(e)
		if err != nil {
			return reply{Err: err.Error()}
		}
		f := r.Apply(data, timeout)
		if wait(f, timeout) != nil {
			return reply{Retry: true}
		}
		res, _ := f.Response().(room.Result)
		switch {
		case res.Stale:
			continue
		case res.Err != "":
			return reply{Err: res.Err}
		}
		return reply{Seq: res.Seq}
	}
	return reply{Retry: true}
}

func (p *Player) decide(actor, id string, cmd []byte) reply {
	return p.propose(func(s *room.State) (room.Entry, *room.Result, error) {
		if !time.Now().Before(s.Info().Expires) {
			return room.Entry{}, nil, room.ErrExpired
		}
		return s.Decide(actor, id, cmd)
	})
}

func (p *Player) end(kind string) reply {
	return p.propose(func(s *room.State) (room.Entry, *room.Result, error) {
		switch info := s.Info(); {
		case info.Phase == room.Playing:
			return s.End(kind, p.n.ID().String()), nil, nil
		case kind == room.KindFinish && info.Phase == room.Finished, kind == room.KindExpire && info.Phase == room.Expired:
			return room.Entry{}, &room.Result{Seq: info.Version}, nil // a retry of an end that counted
		}
		return room.Entry{}, nil, room.ErrOver
	})
}

// serveSubmit answers a member's command: decided here if this node is host.
func (p *Player) serveSubmit(from peer.ID, q request) reply {
	p.mu.Lock()
	r, member := p.raft, p.roster[from]
	p.mu.Unlock()
	switch {
	case r == nil:
		return reply{Err: errNotStarted.Error()}
	case !member:
		return reply{Err: "not a player in this game"}
	case r.State() != raft.Leader:
		return p.notHost(r)
	}
	return p.decide(from.String(), q.ID, q.Body)
}

// handOver asks a random other voter to take over at once, instead of letting
// the others wait for a crash to be detected. With no other voter (two players)
// the game ends: the guest is told to save it as interrupted too.
func (p *Player) handOver(r *raft.Raft) {
	if r.State() != raft.Leader {
		return
	}
	f := r.GetConfiguration()
	if wait(f, timeout) != nil {
		return
	}
	var others []raft.Server
	for _, s := range f.Configuration().Servers {
		if s.Suffrage == raft.Voter && string(s.ID) != p.self {
			others = append(others, s)
		}
	}
	if len(others) > 0 {
		s := others[rand.IntN(len(others))]
		_ = wait(r.LeadershipTransferToServer(s.ID, s.Address), timeout)
		return
	}
	p.mu.Lock()
	info := p.state.Info()
	p.mu.Unlock()
	for _, m := range info.Roster[1:] {
		if id, err := peer.Decode(m); err == nil {
			_, _ = ask(context.Background(), p.n, id, request{Op: "closed", Room: p.id, Seq: info.Version})
		}
	}
}

// serveClosed ends a two-player game whose host closed its program, once this
// copy holds what the host committed (or after 2 s).
func (p *Player) serveClosed(from peer.ID, q request) reply {
	p.mu.Lock()
	ok := p.state != nil && p.state.Info().Players == 2 && p.state.Info().Roster[0] == from.String()
	p.mu.Unlock()
	if !ok {
		return reply{Err: "only the host of a two-player game can close it"}
	}
	p.catchUp(q.Seq, 2*time.Second)
	p.stop(true)
	return reply{}
}

// expire ends the game at its deadline. Whoever is host then proposes it;
// Submit refuses commands locally from the deadline on even without a host.
func (p *Player) expire(at time.Time) {
	t := time.NewTimer(time.Until(at))
	defer t.Stop()
	select {
	case <-p.done:
		return
	case <-t.C:
	}
	p.signal()
	tick := time.NewTicker(p.timing().Heartbeat)
	defer tick.Stop()
	for p.Status().Phase == room.Playing {
		if r := p.replica(); r != nil && r.State() == raft.Leader {
			p.end(room.KindExpire)
		}
		select {
		case <-p.done:
			return
		case <-tick.C:
		}
	}
}
