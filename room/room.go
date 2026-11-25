// Package room holds the rules of a player-hosted room: who may sit down before
// the game starts, and what each event does once the players agree on its order.
// It does no network IO and reads no clock while applying events, so every
// player's copy reaches the same state. Package lan keeps the copies in step.
package room

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

const (
	MinPlayers = 2
	MaxPlayers = 32
	MaxTTL     = 72 * time.Hour
	MaxPayload = 4096
	MaxEvents  = 4096 // per room, including the start event
	maxID      = 128
)

var (
	ErrExpired = errors.New("room expired")
	ErrOver    = errors.New("game is over")
	ErrFull    = errors.New("room full")
	ErrInvite  = errors.New("invalid room invite")
	ErrWaiting = errors.New("waiting for players")
	ErrStarted = errors.New("already started")
)

// Phase of a started game. Before the start a room is a Lobby.
type Phase string

const (
	Playing  Phase = "playing"
	Finished Phase = "finished"
	Expired  Phase = "expired"
)

// Event and entry kinds.
const (
	KindStart  = "start"
	KindGame   = "game"
	KindFinish = "finish"
	KindExpire = "expire"
)

// Member is one seat. Peer must be the identity authenticated by the transport,
// never one taken from a message.
type Member struct {
	Peer  string
	Addrs []string `json:",omitempty"` // where the other members can reach it
	Node  string   `json:",omitempty"` // replication address of this run of the member's program
	Voter bool     `json:",omitempty"` // may take part in choosing a host
}

// Setup is everything a member needs to begin a game. It is fixed at the start
// and identical on every member. Roster[0] created the room.
type Setup struct {
	ID               string
	Players          int
	Created, Expires time.Time
	Roster           []Member
}

// Event is one committed step of a game. Seq 1 is always the start.
type Event struct {
	Seq       int
	Kind      string
	Peer      string
	CommandID string `json:",omitempty"`
	Body      []byte `json:",omitempty"`
}

// Token returns a random room ID or invite secret.
func Token() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// validID keeps IDs usable as file names on every member's machine.
func validID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
