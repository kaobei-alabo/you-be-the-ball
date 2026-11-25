// Package room serializes membership and committed events for one player-hosted room.
// It knows nothing about networks or the meaning of game payloads.
package room

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"
)

const (
	MaxTTL     = 72 * time.Hour
	MaxPayload = 4096
	PageSize   = 32
	maxEvents  = 4096 // small, bounded LAN games; do not silently discard retry records
)

var ErrExpired = errors.New("room expired")

// Game callbacks must be deterministic (Apply), quick, and must not re-enter the room.
type Game interface {
	Initial() []byte
	Decide(state []byte, actor string, command []byte) ([]byte, error)
	Apply(state []byte, event []byte) ([]byte, error)
}

type Config struct {
	Host    string
	Players int
	TTL     time.Duration
	Private bool
	Now     func() time.Time
}

type Info struct {
	ID, Host       string
	Players        int
	Expires        time.Time
	Started        bool
	Version        int
	Roster, Online []string
}

type Event struct {
	Seq        int
	Kind, Peer string
	CommandID  string   `json:",omitempty"`
	Body       []byte   `json:",omitempty"`
	Roster     []string `json:",omitempty"`
}

type Page struct {
	Info   Info
	Events []Event
}

type record struct {
	ID, Host, Secret string
	Players          int
	Created, Expires time.Time
	Events           []Event
}

type receipt struct{ peer, id string }
type Room struct {
	mu       sync.Mutex
	r        record
	file     string
	now      func() time.Time
	game     Game
	state    []byte
	online   map[string]uint64
	next     uint64
	receipts map[receipt]int
	timer    *time.Timer
	closed   bool
	expired  bool
}

// Seat is a capability for one authenticated connection. Rejoining supersedes the old seat.
type Seat struct {
	room *Room
	peer string
	n    uint64
}

func Token() string { var b [16]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

// Open resumes an unexpired room, or creates one if absent/expired. Use one owner per
// directory. Passing an empty directory selects volatile storage (for demos/tests).
func Open(dir string, c Config, game Game) (*Room, error) {
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.TTL == 0 {
		c.TTL = MaxTTL
	}
	if c.Host == "" || len(c.Host) > 256 || c.Players < 2 || c.Players > 32 || c.TTL <= 0 || c.TTL > MaxTTL || game == nil {
		return nil, errors.New("require a host, game, 2..32 players, and TTL in (0,72h]")
	}
	r := &Room{now: c.Now, game: game, state: bytes.Clone(game.Initial()), online: map[string]uint64{c.Host: 1}, next: 1, receipts: make(map[receipt]int)}
	old, file, err := load(dir)
	if err != nil {
		return nil, err
	}
	r.file = file
	now := c.Now().UTC()
	if old != nil && !now.Before(old.Expires) {
		if err := remove(file); err != nil {
			return nil, err
		}
		old = nil
	}
	if old == nil {
		r.r = record{ID: Token(), Host: c.Host, Players: c.Players, Created: now, Expires: now.Add(c.TTL)}
		if c.Private {
			r.r.Secret = Token()
		}
		if err := save(file, r.r); err != nil {
			return nil, err
		}
	} else {
		if old.Host != c.Host {
			return nil, errors.New("stored room belongs to a different host identity")
		}
		if old.ID == "" || old.Players < 2 || old.Players > 32 || !old.Expires.After(old.Created) || old.Expires.Sub(old.Created) > MaxTTL || len(old.Events) > maxEvents {
			return nil, errors.New("invalid stored room")
		}
		r.r = *old
		for i, e := range old.Events {
			if e.Seq != i+1 || (i == 0 && (e.Kind != "start" || len(e.Roster) != old.Players || !slices.Contains(e.Roster, old.Host))) || (i > 0 && (e.Kind != "game" || e.CommandID == "" || !slices.Contains(old.Events[0].Roster, e.Peer))) {
				return nil, errors.New("invalid stored event log")
			}
			if i > 0 {
				r.state, err = game.Apply(bytes.Clone(r.state), bytes.Clone(e.Body))
				if err != nil {
					return nil, fmt.Errorf("replay %d: %w", e.Seq, err)
				}
				r.receipts[receipt{e.Peer, e.CommandID}] = e.Seq
			}
		}
	}
	r.mu.Lock()
	r.timer = time.AfterFunc(r.r.Expires.Sub(r.now()), r.expire)
	r.mu.Unlock()
	return r, nil
}

func (r *Room) expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if remaining := r.r.Expires.Sub(r.now()); remaining > 0 {
		r.timer.Reset(remaining)
		return
	}
	if err := r.purge(); err != nil {
		r.timer.Reset(time.Second)
	}
}

func (r *Room) purge() error {
	r.expired = true
	r.state = nil
	r.online = nil
	r.receipts = nil
	r.r.Events = nil
	r.r.Secret = ""
	return remove(r.file)
}

func (r *Room) check() error {
	if r.closed {
		return errors.New("room closed")
	}
	if r.expired || !r.now().Before(r.r.Expires) {
		return errors.Join(ErrExpired, r.purge())
	}
	return nil
}

func (r *Room) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.timer.Stop()
	r.state = nil
	r.r.Events = nil
	r.r.Secret = ""
	r.receipts = nil
	r.online = nil
}

func (r *Room) status() Info {
	online := make([]string, 0, len(r.online))
	for p := range r.online {
		online = append(online, p)
	}
	sort.Strings(online)
	roster := slices.Clone(online)
	started := len(r.r.Events) > 0
	if started {
		roster = slices.Clone(r.r.Events[0].Roster)
	}
	return Info{r.r.ID, r.r.Host, r.r.Players, r.r.Expires, started, len(r.r.Events), roster, online}
}

func (r *Room) View() (Info, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(); err != nil {
		return Info{}, nil, err
	}
	return r.status(), bytes.Clone(r.state), nil
}

func (r *Room) Invite() (id, secret string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err = r.check(); err != nil {
		return
	}
	return r.r.ID, r.r.Secret, nil
}

func (r *Room) Listing() (Info, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(); err != nil {
		return Info{}, err
	}
	if r.r.Secret != "" || len(r.r.Events) > 0 || len(r.online) >= r.r.Players {
		return Info{}, errors.New("no open public room")
	}
	return r.status(), nil
}

// Join must receive the identity authenticated by the transport, never one from a message.
func (r *Room) Join(peer, id, secret string) (*Seat, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(); err != nil {
		return nil, err
	}
	a, b := sha256.Sum256([]byte(secret)), sha256.Sum256([]byte(r.r.Secret))
	if id != r.r.ID || subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
		return nil, errors.New("invalid room invite")
	}
	if peer == "" || len(peer) > 256 || peer == r.r.Host {
		return nil, errors.New("invalid guest identity")
	}
	if len(r.r.Events) > 0 {
		if !slices.Contains(r.r.Events[0].Roster, peer) {
			return nil, errors.New("roster is locked")
		}
	} else if _, ok := r.online[peer]; !ok && len(r.online) >= r.r.Players {
		return nil, errors.New("room full")
	}
	r.next++
	r.online[peer] = r.next
	return &Seat{r, peer, r.next}, nil
}

func (s *Seat) Leave() {
	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.online[s.peer] == s.n {
		delete(r.online, s.peer)
	}
}

func (s *Seat) active() error {
	if err := s.room.check(); err != nil {
		return err
	}
	if s.room.online[s.peer] != s.n {
		return errors.New("session no longer active")
	}
	return nil
}

func (r *Room) commit(e Event) error {
	next := r.r
	next.Events = append(r.r.Events, e)
	if err := save(r.file, next); err != nil {
		return err
	}
	r.r = next
	return nil
}

// Start is a host-local operation; the guest protocol does not expose it.
func (r *Room) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(); err != nil {
		return err
	}
	if len(r.r.Events) > 0 {
		return errors.New("already started")
	}
	if len(r.online) != r.r.Players {
		return errors.New("waiting for players")
	}
	return r.commit(Event{Seq: 1, Kind: "start", Peer: r.r.Host, Roster: r.status().Roster})
}

func (r *Room) Submit(id string, cmd []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(); err != nil {
		return 0, err
	}
	return r.submit(r.r.Host, id, cmd)
}

func (s *Seat) Submit(id string, cmd []byte) (int, error) {
	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.active(); err != nil {
		return 0, err
	}
	return r.submit(s.peer, id, cmd)
}

func (r *Room) submit(peer, id string, cmd []byte) (int, error) {
	if len(r.r.Events) == 0 {
		return 0, errors.New("room not started")
	}
	if id == "" || len(id) > 128 || len(cmd) > MaxPayload {
		return 0, errors.New("invalid command ID or payload size")
	}
	key := receipt{peer, id}
	if seq, ok := r.receipts[key]; ok {
		return seq, nil
	}
	if len(r.r.Events) >= maxEvents {
		return 0, errors.New("room event limit reached")
	}
	body, err := r.game.Decide(bytes.Clone(r.state), peer, bytes.Clone(cmd))
	if err != nil {
		return 0, err
	}
	if len(body) > MaxPayload {
		return 0, errors.New("event too large")
	}
	body = bytes.Clone(body)
	state, err := r.game.Apply(bytes.Clone(r.state), bytes.Clone(body))
	if err != nil {
		return 0, err
	}
	seq := len(r.r.Events) + 1
	if err = r.commit(Event{Seq: seq, Kind: "game", Peer: peer, CommandID: id, Body: body}); err != nil {
		return 0, err
	}
	r.state = state
	r.receipts[key] = seq
	return seq, nil
}

func (s *Seat) Since(after int) (Page, error) {
	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.active(); err != nil {
		return Page{}, err
	}
	if after < 0 || after > len(r.r.Events) {
		return Page{}, errors.New("invalid replay cursor")
	}
	events := slices.Clone(r.r.Events[after:min(after+PageSize, len(r.r.Events))])
	for i := range events {
		events[i].Body = bytes.Clone(events[i].Body)
		events[i].Roster = slices.Clone(events[i].Roster)
	}
	return Page{r.status(), events}, nil
}
