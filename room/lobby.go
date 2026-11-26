package room

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"sort"
	"sync"
	"time"
)

// Config describes a new room. Players counts the host.
type Config struct {
	Host    string
	Players int
	TTL     time.Duration
	Private bool
	Now     func() time.Time
}

// LobbyInfo is what players see before the start.
type LobbyInfo struct {
	ID, Host string
	Players  int
	Expires  time.Time
	Online   []string
}

// Lobby is a room before its game starts. Only the creator's process holds it,
// so losing the creator closes the room. It is safe for concurrent use.
type Lobby struct {
	mu               sync.Mutex
	id, secret, host string
	players          int
	created, expires time.Time
	now              func() time.Time
	seats            map[string]seat
	next             uint64
	closed           bool
}

type seat struct {
	member Member
	n      uint64
}

// Seat is a capability for one authenticated connection. Rejoining supersedes
// the old seat, so a stale connection cannot free the new one.
type Seat struct {
	lobby *Lobby
	peer  string
	n     uint64
}

func NewLobby(c Config) (*Lobby, error) {
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.TTL == 0 {
		c.TTL = MaxTTL
	}
	if c.Host == "" || len(c.Host) > 256 || c.Players < MinPlayers || c.Players > MaxPlayers || c.TTL <= 0 || c.TTL > MaxTTL {
		return nil, errors.New("require a host, 2..32 players, and TTL in (0,72h]")
	}
	now := c.Now().UTC()
	l := &Lobby{id: Token(), host: c.Host, players: c.Players, created: now, expires: now.Add(c.TTL), now: c.Now, seats: make(map[string]seat)}
	if c.Private {
		l.secret = Token()
	}
	return l, nil
}

func (l *Lobby) check() error {
	if l.closed {
		return errors.New("lobby closed")
	}
	if !l.now().Before(l.expires) {
		l.closed = true
		l.seats = nil
		l.secret = ""
		return ErrExpired
	}
	return nil
}

func (l *Lobby) info() LobbyInfo {
	online := make([]string, 0, len(l.seats)+1)
	online = append(online, l.host)
	for p := range l.seats {
		online = append(online, p)
	}
	sort.Strings(online)
	return LobbyInfo{ID: l.id, Host: l.host, Players: l.players, Expires: l.expires, Online: online}
}

func (l *Lobby) Info() (LobbyInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.check(); err != nil {
		return LobbyInfo{}, err
	}
	return l.info(), nil
}

func (l *Lobby) Invite() (id, secret string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err = l.check(); err != nil {
		return
	}
	return l.id, l.secret, nil
}

// Listing reports the lobby only if it is public and has a free seat. It does
// not take a seat.
func (l *Lobby) Listing() (LobbyInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.check(); err != nil {
		return LobbyInfo{}, err
	}
	if l.secret != "" || len(l.seats)+1 >= l.players {
		return LobbyInfo{}, errors.New("no open public room")
	}
	return l.info(), nil
}

// Join seats a guest. m.Peer must be the identity authenticated by the transport.
func (l *Lobby) Join(m Member, id, secret string) (*Seat, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.check(); err != nil {
		return nil, err
	}
	a, b := sha256.Sum256([]byte(secret)), sha256.Sum256([]byte(l.secret))
	if id != l.id || subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
		return nil, ErrInvite
	}
	if m.Peer == "" || len(m.Peer) > 256 || m.Peer == l.host {
		return nil, errors.New("invalid guest identity")
	}
	if _, ok := l.seats[m.Peer]; !ok && len(l.seats)+1 >= l.players {
		return nil, ErrFull
	}
	l.next++
	l.seats[m.Peer] = seat{m, l.next}
	return &Seat{l, m.Peer, l.next}, nil
}

// Leave frees the seat unless a newer connection of the same peer holds it.
func (s *Seat) Leave() {
	l := s.lobby
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seats[s.peer].n == s.n {
		delete(l.seats, s.peer)
	}
}

// Start fixes the roster and closes the lobby. It needs every seat taken. With
// two players only the host may be chosen as host: a lone survivor cannot tell a
// crash from a dropped connection, so it must never take over.
func (l *Lobby) Start(host Member) (Setup, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.check(); err != nil {
		return Setup{}, err
	}
	if host.Peer != l.host {
		return Setup{}, errors.New("only the host can start")
	}
	if len(l.seats)+1 != l.players {
		return Setup{}, ErrWaiting
	}
	roster := make([]Member, 0, l.players)
	for _, s := range l.seats {
		roster = append(roster, s.member)
	}
	sort.Slice(roster, func(i, j int) bool { return roster[i].Peer < roster[j].Peer })
	roster = append([]Member{host}, roster...)
	for i := range roster {
		roster[i].Voter = l.players > 2 || i == 0
	}
	l.closed = true
	l.seats = nil
	l.secret = ""
	return Setup{ID: l.id, Players: l.players, Created: l.created, Expires: l.expires, Roster: roster}, nil
}

func (l *Lobby) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.seats = nil
	l.secret = ""
}
