package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game"
	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/lan"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// play is everything game-specific in the CLI. A second game needs its own
// command word, move and state label here.
var play = struct {
	game.Game
	word  string // REPL command that makes a move
	move  []byte
	label string // printed before the state in "status" and "replay"
}{counter.Game{}, "inc", []byte("inc"), "counter"}

// view renders a state as "status" and "replay" print it.
func view(state []byte) string { return play.label + "=" + string(state) }

// out serializes the REPL's answers and the notices.
type out struct {
	mu sync.Mutex
	w  io.Writer
}

func (o *out) println(a ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintln(o.w, a...)
}

func short(id, me string) string {
	switch {
	case id == "":
		return "none"
	case id == me:
		return "me"
	}
	return id[max(0, len(id)-8):]
}

func repl(p *lan.Player, n *lan.Node, creator bool, stdin io.Reader, stdout io.Writer) error {
	o := &out{w: stdout}
	me := n.ID().String()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); notices(p, me, o, stop) }()
	defer func() { close(stop); <-done }()
	o.println("Commands: status | start (host) |", play.word, "[request-id] | finish (host) | invite | offline | online | quit")
	scan := bufio.NewScanner(stdin)
	for scan.Scan() {
		f := strings.Fields(scan.Text())
		if len(f) == 0 {
			continue
		}
		var err error
		switch f[0] {
		case "quit", "exit":
			return nil
		case "status":
			o.println(status(p, me))
		case "start":
			if !creator {
				err = errors.New("only the host can start")
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err = p.Start(ctx); err == nil {
				o.println("Started")
			}
			cancel()
		case play.word:
			id := room.Token()
			if len(f) > 1 {
				id = f[1]
			}
			ctx, cancel := context.WithTimeout(context.Background(), moveTimeout)
			seq, e := p.Submit(ctx, id, play.move)
			cancel()
			if err = e; err == nil {
				o.println(fmt.Sprintf("committed %s at v%d", id, seq))
			} else if len(f) == 1 && errors.Is(e, lan.ErrNoHost) {
				o.println("request ID (reuse if retrying):", id)
			}
		case "finish":
			ctx, cancel := context.WithTimeout(context.Background(), moveTimeout)
			if err = p.Finish(ctx); err == nil {
				o.println("Finished")
			}
			cancel()
		case "invite":
			var inv lan.Invite
			if inv, err = p.Invite(); err == nil {
				var text string
				if text, err = inv.Encode(); err == nil {
					o.println("Invite:", text)
				}
			}
		case "offline", "online":
			n.SetOffline(f[0] == "offline")
			o.println(strings.ToUpper(f[0][:1]) + f[0][1:])
		default:
			err = errors.New("unknown command")
		}
		if err != nil {
			o.println("error:", err)
		}
	}
	return scan.Err()
}

func status(p *lan.Player, me string) string {
	s := p.Status()
	expires := s.Expires.Format(time.RFC3339)
	switch {
	case s.Phase == lan.Lobby:
		return fmt.Sprintf("phase=lobby online=%d/%d expires=%s", len(s.Online), s.Players, expires)
	case p.State() == nil:
		return "phase=" + string(s.Phase) // the lobby closed
	}
	line := fmt.Sprintf("phase=%s host=%s version=%d %s expires=%s", s.Phase, short(s.Host, me), s.Version, view(p.State()), expires)
	if s.Paused {
		line += " paused"
	}
	if s.Record != "" {
		line += " record=" + s.Record
	}
	return line
}

// notices prints each change of phase or host once, as it happens. A pause is
// told only if it outlasts pauseNotice, so a quick host change shows only the new host.
func notices(p *lan.Player, me string, o *out, stop <-chan struct{}) {
	last := p.Status()
	host, seen := last.Host, false // the creator hosts first; seen: a host was known in play
	var pause <-chan time.Time
	for {
		select {
		case <-stop:
			return
		case <-p.Changes():
		case <-pause:
			pause = nil
			if p.Status().Paused {
				o.println("* paused: no host reachable")
				host = "" // a host after the pause is news, even the same one
			}
			continue
		}
		s := p.Status()
		playing := s.Phase == room.Playing
		if last.Phase == lan.Lobby && s.Phase != lan.Lobby && s.Phase != lan.Closed {
			o.println("* game started")
		}
		// A guest has no host for a moment right after the start; that is no pause.
		if s.Paused && !last.Paused && seen {
			pause = time.After(pauseNotice)
		}
		if playing && s.Host != "" {
			if s.Host != host {
				o.println("* host is now", short(s.Host, me))
			}
			host, seen = s.Host, true
		}
		if last.Record == "" && s.Record != "" && s.Phase == room.Finished {
			o.println("* game finished; record saved to", s.Record)
		}
		if last.Phase != room.Expired && s.Phase == room.Expired {
			o.println("* game expired")
		}
		if last.Phase != lan.Closed && s.Phase == lan.Closed {
			o.println("* room closed")
		}
		last = s
	}
}
