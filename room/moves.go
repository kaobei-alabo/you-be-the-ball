package room

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game"
)

// Move is one applied step of a game, as kept for replays.
type Move struct {
	Seq  int
	Kind string `json:",omitempty"` // empty for a game move; KindFinish or KindExpire for the end
	Peer string `json:",omitempty"`
	ID   string `json:",omitempty"` // the mover's command ID
	Body []byte `json:",omitempty"` // the game event
}

// MovesInfo is the first line of a moves file.
type MovesInfo struct {
	ID               string
	Players          []string
	Created, Expires time.Time
}

// Moves appends a game's moves to dir/games/<ID>.moves, one JSON line each, so
// the game can be replayed for a while after it is over. A game may take any
// number of moves, so they are not held in memory: each is appended as it is
// applied. Appending is buffered and never forced to disk per move, so it does
// not slow the game. Not safe for concurrent use.
type Moves struct {
	f    *os.File
	w    *bufio.Writer
	last int // highest Seq in the file
	err  error
}

func movesFile(dir, id string) string { return filepath.Join(dir, "games", id+".moves") }

// OpenMoves starts a game's moves file, or continues the one an earlier run of
// this player left, so a restarted player does not write a move twice.
func OpenMoves(dir string, s Setup) (*Moves, error) {
	if !validID(s.ID) {
		return nil, errors.New("invalid room ID")
	}
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(movesFile(dir, s.ID), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	m := &Moves{f: f, last: 1}
	if err = m.resume(s); err != nil {
		f.Close()
		return nil, err
	}
	m.w = bufio.NewWriter(f)
	return m, nil
}

// resume positions the file after its last whole line, writing the first line
// if there is none. A line cut short by a crash is dropped.
func (m *Moves) resume(s Setup) error {
	size, err := m.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// The last move is in the tail: a line is at most a few kilobytes.
	tail := make([]byte, min(size, 64<<10))
	if _, err = m.f.ReadAt(tail, size-int64(len(tail))); err != nil && err != io.EOF {
		return err
	}
	whole := bytes.LastIndexByte(tail, '\n') + 1
	if whole == 0 && size > int64(len(tail)) {
		return errors.New("not a moves file") // no line is that long
	}
	size -= int64(len(tail) - whole)
	if err = m.f.Truncate(size); err != nil {
		return err
	}
	if _, err = m.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if size == 0 {
		players := make([]string, len(s.Roster))
		for i, p := range s.Roster {
			players[i] = p.Peer
		}
		b, _ := json.Marshal(MovesInfo{ID: s.ID, Players: players, Created: s.Created, Expires: s.Expires})
		_, err = m.f.Write(append(b, '\n'))
		return err
	}
	lines := bytes.Split(tail[:whole-1], []byte("\n"))
	var mv Move
	if json.Unmarshal(lines[len(lines)-1], &mv) == nil && mv.Seq > 1 {
		m.last = mv.Seq
	}
	return nil
}

// Add appends a move. One this file already holds is skipped. After a write
// error nothing more is written: a replay is a convenience and must never
// stop the game.
func (m *Moves) Add(mv Move) {
	if m.err != nil || mv.Seq <= m.last {
		return
	}
	b, err := json.Marshal(mv)
	if err == nil {
		_, err = m.w.Write(append(b, '\n'))
	}
	m.last, m.err = mv.Seq, err
}

// Flush hands buffered moves to the operating system.
func (m *Moves) Flush() error {
	if m.err == nil {
		m.err = m.w.Flush()
	}
	return m.err
}

// Close flushes, forces the file to disk once, and closes it.
func (m *Moves) Close() error {
	err := m.Flush()
	if err == nil {
		err = m.f.Sync()
	}
	if closeErr := m.f.Close(); err == nil {
		err = closeErr
	}
	if m.err == nil {
		m.err = errors.New("moves file closed")
	}
	return err
}

// Replay reads a moves file and calls each with every move and the game's
// state after it. It fails on a gap: a player that was away while moves were
// made does not have them, though the other players do. The game's record, if
// there is one next to the file, tells whether moves are missing at the end.
func Replay(g game.Game, file string, each func(Move, []byte) error) (MovesInfo, error) {
	var info MovesInfo
	f, err := os.Open(file)
	if err != nil {
		return info, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	if !sc.Scan() || json.Unmarshal(sc.Bytes(), &info) != nil || !validID(info.ID) {
		return info, errors.New("not a moves file")
	}
	state, seq := bytes.Clone(g.Initial()), 1
	for sc.Scan() {
		var mv Move
		if err = json.Unmarshal(sc.Bytes(), &mv); err != nil {
			if !sc.Scan() && sc.Err() == nil {
				break // the last line, cut short by a crash
			}
			return info, fmt.Errorf("after move %d: %w", seq, err)
		}
		if mv.Seq != seq+1 {
			return info, missing(seq+1, mv.Seq-1)
		}
		seq = mv.Seq
		if mv.Kind == "" {
			if state, err = g.Apply(state, mv.Body); err != nil {
				return info, fmt.Errorf("move %d: %w", seq, err)
			}
		}
		if err = each(mv, bytes.Clone(state)); err != nil {
			return info, err
		}
	}
	if err = sc.Err(); err != nil {
		return info, err
	}
	// A player brought up to date from a snapshot of a game already over, or
	// that left before its next move, has no line telling it missed the end.
	if r, err := ReadRecord(strings.TrimSuffix(file, ".moves") + ".json"); err == nil && r.ID == info.ID && r.Version > seq {
		return info, missing(seq+1, r.Version)
	}
	return info, nil
}

func missing(from, to int) error {
	return fmt.Errorf("moves %d to %d are missing: this player was away then; another player's file has them", from, to)
}

// PurgeMoves deletes the moves files in dir/games that were last written more
// than keep ago and returns how many. A game still being played is never old
// enough: a room lasts at most MaxTTL, and keep below that only applies once
// the game's result has been saved ("interrupted" is not one until the room
// expires: the player may have rejoined).
func PurgeMoves(dir string, keep time.Duration, now time.Time) (int, error) {
	entries, err := os.ReadDir(filepath.Join(dir, "games"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), ".moves")
		info, err := e.Info()
		if !ok || err != nil || now.Sub(info.ModTime()) < keep {
			continue
		}
		if now.Sub(info.ModTime()) < MaxTTL {
			r, err := ReadRecord(filepath.Join(dir, "games", id+".json"))
			if err != nil || r.Status == "interrupted" && now.Before(r.Expires) {
				continue // no result yet: the game may still be going
			}
		}
		if os.Remove(filepath.Join(dir, "games", e.Name())) == nil {
			n++
		}
	}
	return n, nil
}
