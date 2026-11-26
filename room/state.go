package room

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game"
)

// Entry is a proposed step. The host builds it with Decide or End; once the
// players agree on its position, every member passes it to Apply.
type Entry struct {
	Kind      string
	Peer      string
	CommandID string `json:",omitempty"`
	Body      []byte `json:",omitempty"`
	Based     int    `json:",omitempty"` // version the host decided against
	Round     string `json:",omitempty"` // the draft it was decided in
	N         int    `json:",omitempty"` // its place in that draft, from 0
}

// Result is the outcome of applying an entry. It is the same on every member.
type Result struct {
	Seq    int    `json:",omitempty"` // position of the step; the original one for a repeat
	Repeat bool   `json:",omitempty"` // the command had been applied before
	Stale  bool   `json:",omitempty"` // decided against an older version: decide again
	Err    string `json:",omitempty"` // rejected; nothing changed
}

// Info summarises a started game.
type Info struct {
	ID      string
	Players int
	Expires time.Time
	Phase   Phase
	Version int
	Roster  []string
}

type receipt struct{ peer, id string }

// State is one member's copy of a started game. Apply is a pure function of the
// state and the entry: it reads no clock and does no IO, so members that apply
// the same entries in the same order hold the same state. State is not safe for
// concurrent use; the caller serializes access.
//
// A game may take any number of moves, so the moves themselves are not kept:
// only the game's current state, how many steps led to it, and each player's
// most recent receipts.
type State struct {
	setup    Setup
	game     game.Game
	phase    Phase
	version  int // 1 is the start; every applied step adds one
	state    []byte
	receipts map[receipt]int
	recent   map[string][]string // per player, oldest first: the IDs in receipts
	round    string              // Round and N of the entry applied last
	n        int
}

func checkSetup(s Setup, g game.Game) error {
	if g == nil || !validID(s.ID) || s.Players < MinPlayers || s.Players > MaxPlayers || len(s.Roster) != s.Players {
		return errors.New("invalid room setup")
	}
	if !s.Expires.After(s.Created) || s.Expires.Sub(s.Created) > MaxTTL {
		return errors.New("invalid room expiry")
	}
	seen := make(map[string]bool, len(s.Roster))
	for _, m := range s.Roster {
		if m.Peer == "" || len(m.Peer) > 256 || seen[m.Peer] {
			return errors.New("invalid room roster")
		}
		seen[m.Peer] = true
	}
	return nil
}

func cloneSetup(s Setup) Setup {
	s.Roster = slices.Clone(s.Roster)
	for i := range s.Roster {
		s.Roster[i].Addrs = slices.Clone(s.Roster[i].Addrs)
	}
	return s
}

// NewState begins a game. Every member calls it with the same Setup.
func NewState(s Setup, g game.Game) (*State, error) {
	if err := checkSetup(s, g); err != nil {
		return nil, err
	}
	return &State{
		setup:    cloneSetup(s),
		game:     g,
		phase:    Playing,
		version:  1,
		state:    bytes.Clone(g.Initial()),
		receipts: make(map[receipt]int),
		recent:   make(map[string][]string),
	}, nil
}

func (s *State) member(peer string) bool {
	return slices.ContainsFunc(s.setup.Roster, func(m Member) bool { return m.Peer == peer })
}

// remember keeps a receipt and forgets the player's oldest beyond the window.
func (s *State) remember(peer, id string, seq int) {
	s.receipts[receipt{peer, id}] = seq
	ids := append(s.recent[peer], id)
	if len(ids) > Receipts {
		delete(s.receipts, receipt{peer, ids[0]})
		ids = ids[1:]
	}
	s.recent[peer] = ids
}

// end closes the game. The moves' receipts go with it: once a game is over only
// its result is kept.
func (s *State) end(p Phase) int {
	s.phase = p
	s.version++
	s.receipts, s.recent = make(map[receipt]int), make(map[string][]string)
	return s.version
}

// Receipt is the sequence number of a command that was applied, if it is one of
// the player's most recent Receipts commands of a game still being played.
func (s *State) Receipt(peer, id string) (int, bool) {
	seq, ok := s.receipts[receipt{peer, id}]
	return seq, ok
}

// Decide validates a command against the current state and returns the entry to
// propose. If the command was applied before, it returns that result instead and
// no entry, so a retry gets its original receipt. Only the host calls Decide.
func (s *State) Decide(actor, id string, cmd []byte) (Entry, *Result, error) {
	return s.Draft().Decide(actor, id, cmd)
}

// End returns the entry that finishes (KindFinish) or expires (KindExpire) the game.
func (s *State) End(kind, peer string) Entry {
	return Entry{Kind: kind, Peer: peer, Based: s.version}
}

// Draft decides several commands in a row before any of them is agreed on:
// each is decided against the state the earlier ones will leave. The host uses
// one per round, so players who move at the same time are agreed on together
// and none waits for another's move to go round first. A draft never changes
// the State: only Apply does, when the entries are agreed. Each entry after the
// first names the one before it (Round and N), and Apply takes it only directly
// after that one. So if an entry of a round is not applied after all, or
// another host's entry gets in between, the rest of the round is reported
// Stale on every copy and decided again.
type Draft struct {
	s       *State
	phase   Phase
	version int
	state   []byte
	decided map[receipt]bool
	round   string
	n       int
}

// ErrBusy means the answer depends on an entry that is not agreed on yet: ask again.
var ErrBusy = errors.New("still being decided; retry")

func (s *State) Draft() *Draft {
	return &Draft{s: s, phase: s.phase, version: s.version, state: s.state}
}

// link places an entry in the draft's chain.
func (d *Draft) link(e Entry) Entry {
	if d.round == "" {
		d.round = Token()[:16]
	}
	e.Round, e.N = d.round, d.n
	d.n++
	return e
}

// Decide is State.Decide against the draft, and adds the entry to it.
func (d *Draft) Decide(actor, id string, cmd []byte) (Entry, *Result, error) {
	if seq, ok := d.s.Receipt(actor, id); ok {
		return Entry{}, &Result{Seq: seq, Repeat: true}, nil
	}
	if d.decided[receipt{actor, id}] || d.phase != d.s.phase {
		return Entry{}, nil, ErrBusy // the same command, or the end, is in this draft
	}
	if d.phase != Playing {
		return Entry{}, nil, ErrOver
	}
	if !d.s.member(actor) {
		return Entry{}, nil, errors.New("not a player in this game")
	}
	if id == "" || len(id) > maxID || len(cmd) > MaxPayload {
		return Entry{}, nil, errors.New("invalid command ID or payload size")
	}
	body, err := d.s.game.Decide(bytes.Clone(d.state), actor, bytes.Clone(cmd))
	if err != nil {
		return Entry{}, nil, err
	}
	if len(body) > MaxPayload {
		return Entry{}, nil, errors.New("event too large")
	}
	// Never propose an event that cannot be applied.
	next, err := d.s.game.Apply(bytes.Clone(d.state), bytes.Clone(body))
	if err != nil {
		return Entry{}, nil, err
	}
	e := d.link(Entry{Kind: KindGame, Peer: actor, CommandID: id, Body: bytes.Clone(body), Based: d.version})
	if d.decided == nil {
		d.decided = make(map[receipt]bool)
	}
	d.decided[receipt{actor, id}] = true
	d.state, d.version = next, d.version+1
	if f, ok := d.s.game.(game.Finisher); ok && f.Finished(bytes.Clone(d.state)) {
		d.phase, d.version = Finished, d.version+1
	}
	return e, nil, nil
}

// End adds the entry that finishes or expires the game. An end that already
// counted is answered with its result; the other kind of end gets ErrOver.
func (d *Draft) End(kind, peer string) (Entry, *Result, error) {
	ended := Finished
	if kind == KindExpire {
		ended = Expired
	}
	switch {
	case d.phase != d.s.phase:
		return Entry{}, nil, ErrBusy // an end is in this draft
	case d.phase == ended:
		return Entry{}, &Result{Seq: d.version, Repeat: true}, nil
	case d.phase != Playing:
		return Entry{}, nil, ErrOver
	}
	e := d.link(Entry{Kind: kind, Peer: peer, Based: d.version})
	d.phase, d.version = ended, d.version+1
	return e, nil, nil
}

// Apply performs an agreed entry. A rejected entry changes nothing and uses no
// sequence number, on every member alike.
func (s *State) Apply(e Entry) Result {
	switch e.Kind {
	case KindGame:
		if seq, ok := s.Receipt(e.Peer, e.CommandID); ok {
			return Result{Seq: seq, Repeat: true}
		}
		if s.phase != Playing {
			return Result{Err: ErrOver.Error()}
		}
		// Another host may have committed something since this was decided, or
		// the entry it was decided after may not have been applied.
		if e.Based != s.version || e.N > 0 && (e.Round != s.round || e.N != s.n+1) {
			return Result{Stale: true}
		}
		if !s.member(e.Peer) || e.CommandID == "" || len(e.CommandID) > maxID || len(e.Body) > MaxPayload {
			return Result{Err: "invalid entry"}
		}
		next, err := s.game.Apply(bytes.Clone(s.state), bytes.Clone(e.Body))
		if err != nil {
			return Result{Err: err.Error()}
		}
		s.state = next
		s.version++
		seq := s.version
		s.round, s.n = e.Round, e.N
		s.remember(e.Peer, e.CommandID, seq)
		if f, ok := s.game.(game.Finisher); ok && f.Finished(bytes.Clone(s.state)) {
			s.end(Finished)
		}
		return Result{Seq: seq}
	case KindFinish, KindExpire:
		if s.phase != Playing {
			return Result{Seq: s.version, Repeat: true}
		}
		s.round, s.n = e.Round, e.N
		if e.Kind == KindExpire {
			return Result{Seq: s.end(Expired)}
		}
		return Result{Seq: s.end(Finished)}
	}
	return Result{Err: "unknown entry"}
}

func (s *State) Info() Info {
	roster := make([]string, len(s.setup.Roster))
	for i, m := range s.setup.Roster {
		roster[i] = m.Peer
	}
	return Info{ID: s.setup.ID, Players: s.setup.Players, Expires: s.setup.Expires, Phase: s.phase, Version: s.version, Roster: roster}
}

func (s *State) Setup() Setup { return cloneSetup(s.setup) }

// View returns a copy of the game's state.
func (s *State) View() []byte { return bytes.Clone(s.state) }

type applied struct {
	ID  string
	Seq int
}

type snapshot struct {
	Setup    Setup
	Phase    Phase
	Version  int
	State    []byte
	Receipts map[string][]applied `json:",omitempty"` // per player, oldest first
	Round    string               `json:",omitempty"` // of the entry applied last
	N        int                  `json:",omitempty"`
}

// Snapshot serializes the whole state. Its size does not grow with the number
// of moves, so the agreed entries behind it can be thrown away and a member
// that fell far behind can be brought up to date from it.
func (s *State) Snapshot() ([]byte, error) {
	p := snapshot{Setup: s.setup, Phase: s.phase, Version: s.version, State: s.state, Receipts: make(map[string][]applied, len(s.recent)), Round: s.round, N: s.n}
	for peer, ids := range s.recent {
		for _, id := range ids {
			p.Receipts[peer] = append(p.Receipts[peer], applied{id, s.receipts[receipt{peer, id}]})
		}
	}
	return json.Marshal(p)
}

// Restore rebuilds a state from a Snapshot.
func Restore(data []byte, g game.Game) (*State, error) {
	var p snapshot
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if err := checkSetup(p.Setup, g); err != nil {
		return nil, err
	}
	if p.Phase != Playing && p.Phase != Finished && p.Phase != Expired || p.Version < 1 || p.N < 0 || len(p.Round) > 64 {
		return nil, errors.New("invalid snapshot")
	}
	s := &State{setup: p.Setup, game: g, phase: p.Phase, version: p.Version, state: p.State, receipts: make(map[receipt]int), recent: make(map[string][]string), round: p.Round, n: p.N}
	for peer, list := range p.Receipts {
		if !s.member(peer) || len(list) > Receipts || p.Phase != Playing {
			return nil, errors.New("invalid snapshot receipts")
		}
		for _, a := range list {
			if _, dup := s.receipts[receipt{peer, a.ID}]; dup || a.ID == "" || len(a.ID) > maxID || a.Seq < 2 || a.Seq > p.Version {
				return nil, errors.New("invalid snapshot receipts")
			}
			s.remember(peer, a.ID, a.Seq)
		}
	}
	return s, nil
}
