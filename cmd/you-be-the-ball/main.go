// Command you-be-the-ball plays the counter game with other players on a LAN.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/internal/home"
	"github.com/kaobei-alabo/you-be-the-ball/lan"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const usage = `you-be-the-ball host|find|join [flags] [invite]
  you-be-the-ball migrate OLD_PROFILE_DIR [OLD_PROFILE_DIR ...]
  you-be-the-ball help
Defaults: ~/.you-be-the-ball/config.json and ~/.you-be-the-ball/{host,join,find}/
Flags: you-be-the-ball host -h`

// Tests shorten these; there are no flags for them.
var (
	timing      lan.Timing // zero: the lan defaults
	moveTimeout = 5 * time.Second
	pauseNotice = time.Second // about two election timeouts
)

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	mode := args[0]
	switch mode {
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, usage)
		return nil
	case "host", "find", "join", "migrate":
	default:
		return errors.New(usage)
	}
	root, err := home.Root()
	if err != nil {
		return err
	}
	if mode == "migrate" {
		if len(args) < 2 {
			return errors.New("usage: you-be-the-ball migrate OLD_PROFILE_DIR [OLD_PROFILE_DIR ...] (stop old processes first)")
		}
		for _, src := range args[1:] {
			dest, err := home.Migrate(root, src)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Moved %s to %s\n", src, dest)
		}
		return nil
	}
	o, err := parseOptions(stderr, root, mode, args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	var invite lan.Invite
	if mode == "join" {
		if invite, err = lan.Parse(o.token); err != nil {
			return err
		}
	}
	fmt.Fprintln(stderr, "Data directory:", o.dir)
	n, err := lan.New(o.dir, o.listen)
	if err != nil {
		return err
	}
	defer n.Close()
	ctx := context.Background()
	opts := lan.Options{Dir: o.dir, Timing: timing}
	var p *lan.Player
	switch mode {
	case "find":
		return find(ctx, n, o.wait, stdout)
	case "host":
		if p, err = lan.Host(n, play.Game, room.Config{Players: o.players, TTL: o.ttl, Private: o.private}, opts); err != nil {
			return err
		}
		defer p.Close()
		inv, err := p.Invite()
		if err != nil {
			return err
		}
		text, err := inv.Encode()
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, "Invite:", text)
		if o.mdns && inv.Secret == "" {
			svc, err := lan.Advertise(n)
			if err != nil {
				fmt.Fprintln(stderr, "mDNS unavailable; use invite:", err)
			} else {
				defer svc.Close()
			}
		}
	case "join":
		jctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		p, err = lan.Join(jctx, n, play.Game, invite, opts)
		cancel()
		if err != nil {
			return err
		}
		defer p.Close()
	}
	return repl(p, n, mode == "host", stdin, stdout)
}

// find lists public lobbies with a free seat; it takes no seat.
func find(ctx context.Context, n *lan.Node, wait time.Duration, stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	found := 0
	err := lan.Browse(ctx, n, func(i lan.Invite, info room.LobbyInfo) {
		if text, err := i.Encode(); err == nil {
			fmt.Fprintf(stdout, "%d/%d players; invite: %s\n", len(info.Online), info.Players, text)
			found++
		}
	})
	if err == nil && found == 0 {
		fmt.Fprintln(stdout, "No open public rooms found. Try the host's invite directly.")
	}
	return err
}
