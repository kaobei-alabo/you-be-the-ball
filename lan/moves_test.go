package lan

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// replayOf plays p's moves file back: each move and the state after it.
func replayOf(p *Player) (moves []room.Move, states []string, err error) {
	_, err = room.Replay(p.g, filepath.Join(p.o.Dir, "games", p.id+".moves"), func(m room.Move, state []byte) error {
		moves, states = append(moves, m), append(states, string(state))
		return nil
	})
	return
}

func finish(t *testing.T, ps []*Player, result string) {
	t.Helper()
	if err := leader(t, ps...).Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	records(t, ps, result)
}

// Every player keeps the whole game, a retry once; a restarted player that
// replays the game from its start writes no move twice.
func TestEveryPlayerKeepsTheMovesAndARestartWritesNoneTwice(t *testing.T) {
	ps, dirs := playDirs(t, room.Config{Players: 3})
	inv := original(t, ps[0])
	inc(t, ps[1], "a")
	inc(t, ps[1], "a") // a retry
	inc(t, ps[2], "b")
	crash(ps[2])
	inc(t, ps[1], "while-away")
	c, err := join(t, newNode(t, dirs[2]), inv, Options{Dir: ps[2].o.Dir, KeepMoves: true, Timing: fast})
	if err != nil {
		t.Fatal(err)
	}
	live := []*Player{ps[0], ps[1], c}
	inc(t, c, "c")
	finish(t, live, "4")
	for _, p := range live {
		moves, states, err := replayOf(p)
		if err != nil || len(moves) != 5 {
			t.Fatal(len(moves), err)
		}
		var ids []string
		for _, m := range moves[:4] {
			ids = append(ids, m.ID)
		}
		if strings.Join(ids, " ") != "a b while-away c" || moves[3].Peer != id(c) || moves[4].Kind != room.KindFinish || moves[4].Seq != 6 || states[4] != "4" {
			t.Fatal("replay of", p.Status().Record, moves, states)
		}
	}
}

// A player brought up to date from a snapshot misses the moves behind it; the
// replay of its file says so, while the others replay the whole game.
func TestReplayOfAPlayerBroughtBackFromASnapshotReportsTheGap(t *testing.T) {
	compactOften(t)
	ps := play(t, room.Config{Players: 3}, fast)
	h := leader(t, ps...)
	off := without(ps, h)[0]
	on := without(ps, off)
	inc(t, off, "mine")
	converged(t, ps, "1")
	_, last, _ := span(off)
	off.n.SetOffline(true)
	moves(t, on, "m", 100)
	waitFor(t, "the host's log is cut past the offline player", func() bool {
		lo, _, _ := span(h)
		return lo > last+1
	})
	off.n.SetOffline(false)
	converged(t, ps, "101")
	inc(t, off, "back")
	finish(t, ps, "102")
	for _, p := range on {
		if ms, states, err := replayOf(p); err != nil || len(ms) != 103 || states[102] != "102" {
			t.Fatal(len(ms), err)
		}
	}
	ms, _, err := replayOf(off)
	if err == nil || !strings.Contains(err.Error(), "are missing") || len(ms) == 0 || ms[0].ID != "mine" {
		t.Fatal("the gap was not reported:", len(ms), err)
	}
}

// toThree ends itself at 3.
type toThree struct{ counter.Game }

func (toThree) Finished(state []byte) bool { return string(state) == "3" }

func TestAGameThatEndsItselfEndsItsMoves(t *testing.T) {
	old := theGame
	theGame = toThree{}
	t.Cleanup(func() { theGame = old })
	ps := play(t, room.Config{Players: 2}, fast)
	for i := range 3 {
		inc(t, ps[i%2], strconv.Itoa(i))
	}
	records(t, ps, "3")
	for _, p := range ps {
		ms, _, err := replayOf(p)
		if err != nil || len(ms) != 4 || ms[3].Seq != 5 || ms[3].Kind != room.KindFinish || ms[3].Peer != "" {
			t.Fatal(ms, err)
		}
	}
}

// The moves reach the file while the game is going, not only at its end, so a
// crash loses only the last second or so.
func TestMovesReachTheFileDuringTheGame(t *testing.T) {
	ps := play(t, room.Config{Players: 2}, fast)
	inc(t, ps[1], "a")
	for _, p := range ps {
		waitFor(t, "the move in the file", func() bool {
			ms, _, _ := replayOf(p)
			return len(ms) == 1 && ms[0].ID == "a"
		})
		if p.Status().Phase != room.Playing {
			t.Fatal("the game ended")
		}
	}
	finish(t, ps, "1")
	for _, p := range ps {
		if ms, _, err := replayOf(p); err != nil || len(ms) != 2 || ms[1].Peer != id(ps[0]) {
			t.Fatal("the finish is not logged with its proposer:", ms, err)
		}
	}
}

// An end entry's proposer is not checked by the state: only a player's ID goes
// to the file, so a host cannot make it unreadable.
func TestAnEndLogsOnlyAPlayersID(t *testing.T) {
	ps := play(t, room.Config{Players: 2}, fast)
	inc(t, ps[1], "a")
	converged(t, ps, "1")
	p := ps[1]
	data, _ := json.Marshal(room.Entry{Kind: room.KindFinish, Peer: strings.Repeat("x", 2<<20)})
	(*fsm)(p).Apply(&raft.Log{Data: data})
	ms, _, err := replayOf(p)
	if err != nil || len(ms) != 2 || ms[1].Kind != room.KindFinish || ms[1].Peer != "" {
		t.Fatal(len(ms), err)
	}
}

func TestNoMovesFileUnlessAsked(t *testing.T) {
	old := keepMoves
	keepMoves = false
	t.Cleanup(func() { keepMoves = old })
	ps := play(t, room.Config{Players: 2}, fast)
	inc(t, ps[1], "a")
	finish(t, ps, "1")
	for _, p := range ps {
		if _, _, err := replayOf(p); err == nil {
			t.Fatal("a moves file was written")
		}
	}
}
