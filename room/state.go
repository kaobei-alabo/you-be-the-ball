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
}

// Result is the outcome of applying an entry. It is the same on every member.
type Result struct {
	Seq    int    `json:",omitempty"` // position of the event; the original one for a repeat
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
type State struct {
	setup    Setup
	game     game.Game
	phase    Phase
	state    []byte
	events   []Event
	receipts map[receipt]int
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
		state:    bytes.Clone(g.Initial()),
		events:   []Event{{Seq: 1, Kind: KindStart, Peer: s.Roster[0].Peer}},
		receipts: make(map[receipt]int),
	}, nil
}

func (s *State) member(peer string) bool {
	return slices.ContainsFunc(s.setup.Roster, func(m Member) bool { return m.Peer == peer })
}

func (s *State) add(e Event) int {
	e.Seq = len(s.events) + 1
	s.events = append(s.events, e)
	return e.Seq
}

// Decide validates a command against the current state and returns the entry to
// propose. If the command was applied before, it returns that result instead and
// no entry, so a retry gets its original receipt. Only the host calls Decide.
func (s *State) Decide(actor, id string, cmd []byte) (Entry, *Result, error) {
	if seq, ok := s.receipts[receipt{actor, id}]; ok {
		return Entry{}, &Result{Seq: seq, Repeat: true}, nil
	}
	if s.phase != Playing {
		return Entry{}, nil, ErrOver
	}
	if !s.member(actor) {
		return Entry{}, nil, errors.New("not a player in this game")
	}
	if id == "" || len(id) > maxID || len(cmd) > MaxPayload {
		return Entry{}, nil, errors.New("invalid command ID or payload size")
	}
	if len(s.events) >= MaxEvents {
		return Entry{}, nil, errors.New("room event limit reached")
	}
	body, err := s.game.Decide(bytes.Clone(s.state), actor, bytes.Clone(cmd))
	if err != nil {
		return Entry{}, nil, err
	}
	if len(body) > MaxPayload {
		return Entry{}, nil, errors.New("event too large")
	}
	// Never propose an event that cannot be applied.
	if _, err = s.game.Apply(bytes.Clone(s.state), bytes.Clone(body)); err != nil {
		return Entry{}, nil, err
	}
	return Entry{Kind: KindGame, Peer: actor, CommandID: id, Body: bytes.Clone(body), Based: len(s.events)}, nil, nil
}

// Receipt returns the sequence number of peer's command id, if it was applied.
func (s *State) Receipt(peer, id string) (int, bool) {
	seq, ok := s.receipts[receipt{peer, id}]
	return seq, ok
}

// End returns the entry that finishes (KindFinish) or expires (KindExpire) the game.
func (s *State) End(kind, peer string) Entry {
	return Entry{Kind: kind, Peer: peer, Based: len(s.events)}
}

// Apply performs an agreed entry. A rejected entry changes nothing and uses no
// sequence number, on every member alike.
func (s *State) Apply(e Entry) Result {
	switch e.Kind {
	case KindGame:
		if seq, ok := s.receipts[receipt{e.Peer, e.CommandID}]; ok {
			return Result{Seq: seq, Repeat: true}
		}
		if s.phase != Playing {
			return Result{Err: ErrOver.Error()}
		}
		// Another host may have committed something since this was decided.
		if e.Based != len(s.events) {
			return Result{Stale: true}
		}
		if !s.member(e.Peer) || e.CommandID == "" || len(e.CommandID) > maxID || len(e.Body) > MaxPayload || len(s.events) >= MaxEvents {
			return Result{Err: "invalid entry"}
		}
		next, err := s.game.Apply(bytes.Clone(s.state), bytes.Clone(e.Body))
		if err != nil {
			return Result{Err: err.Error()}
		}
		s.state = next
		seq := s.add(Event{Kind: KindGame, Peer: e.Peer, CommandID: e.CommandID, Body: bytes.Clone(e.Body)})
		s.receipts[receipt{e.Peer, e.CommandID}] = seq
		if f, ok := s.game.(game.Finisher); ok && f.Finished(bytes.Clone(s.state)) {
			s.phase = Finished
			s.add(Event{Kind: KindFinish, Peer: e.Peer})
		}
		return Result{Seq: seq}
	case KindFinish, KindExpire:
		if s.phase != Playing {
			return Result{Seq: len(s.events), Repeat: true}
		}
		s.phase = Finished
		if e.Kind == KindExpire {
			s.phase = Expired
		}
		return Result{Seq: s.add(Event{Kind: e.Kind, Peer: e.Peer})}
	}
	return Result{Err: "unknown entry"}
}

func (s *State) Info() Info {
	roster := make([]string, len(s.setup.Roster))
	for i, m := range s.setup.Roster {
		roster[i] = m.Peer
	}
	return Info{ID: s.setup.ID, Players: s.setup.Players, Expires: s.setup.Expires, Phase: s.phase, Version: len(s.events), Roster: roster}
}

func (s *State) Setup() Setup { return cloneSetup(s.setup) }

// View returns a copy of the game's state.
func (s *State) View() []byte { return bytes.Clone(s.state) }

// Events returns copies of the events after the given version.
func (s *State) Events(after int) []Event {
	if after < 0 || after > len(s.events) {
		return nil
	}
	out := slices.Clone(s.events[after:])
	for i := range out {
		out[i].Body = bytes.Clone(out[i].Body)
	}
	return out
}

type snapshot struct {
	Setup  Setup
	Phase  Phase
	State  []byte
	Events []Event
}

// Snapshot serializes the whole state, so a member that fell far behind can be
// brought up to date without replaying every entry.
func (s *State) Snapshot() ([]byte, error) {
	return json.Marshal(snapshot{s.setup, s.phase, s.state, s.events})
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
	if p.Phase != Playing && p.Phase != Finished && p.Phase != Expired {
		return nil, errors.New("invalid snapshot phase")
	}
	if len(p.Events) == 0 || len(p.Events) > MaxEvents+1 || p.Events[0].Kind != KindStart {
		return nil, errors.New("invalid snapshot events")
	}
	s := &State{setup: p.Setup, game: g, phase: p.Phase, state: p.State, events: p.Events, receipts: make(map[receipt]int)}
	for i, e := range p.Events {
		if e.Seq != i+1 {
			return nil, errors.New("invalid snapshot events")
		}
		if e.Kind == KindGame {
			s.receipts[receipt{e.Peer, e.CommandID}] = e.Seq
		}
	}
	return s, nil
}
