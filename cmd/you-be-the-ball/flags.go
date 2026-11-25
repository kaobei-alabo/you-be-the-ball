package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/internal/home"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

type options struct {
	dir, listen, token string
	players            int
	ttl, wait          time.Duration
	private, mdns      bool
}

// parseOptions: flags win over config.json, which wins over the defaults.
// The profile defaults to the subcommand's name.
// Flag errors and -h go to out.
func parseOptions(out io.Writer, root, mode string, args []string) (options, error) {
	var o options
	cfg, err := home.LoadConfig(root)
	if err != nil {
		return o, err
	}
	addr := cfg.PeerListen
	if mode == "host" {
		addr = cfg.HostListen
	}
	fs := flag.NewFlagSet("you-be-the-ball "+mode, flag.ContinueOnError)
	fs.SetOutput(out)
	profile := fs.String("profile", mode, "profile name under ~/.you-be-the-ball (one per running player)")
	fs.StringVar(&o.dir, "dir", "", "explicit data directory instead of a home profile (legacy override)")
	fs.StringVar(&o.listen, "listen", addr, "libp2p listen multiaddress")
	fs.IntVar(&o.players, "players", cfg.Players, "players required, including host (new room)")
	fs.DurationVar(&o.ttl, "ttl", room.MaxTTL, "room lifetime (new room)")
	fs.BoolVar(&o.private, "private", cfg.Private, "require invite secret; hide lobby (new room)")
	fs.BoolVar(&o.mdns, "mdns", cfg.MDNS, "advertise the hosting peer via multicast DNS (host only)")
	fs.DurationVar(&o.wait, "wait", 8*time.Second, "LAN browsing duration")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if (mode == "join" && fs.NArg() != 1) || (mode != "join" && fs.NArg() != 0) {
		return o, errors.New("join takes one invite; host and find take flags only")
	}
	if mode == "join" {
		o.token = fs.Arg(0)
	}
	if mode == "host" && (o.players < room.MinPlayers || o.players > room.MaxPlayers) {
		return o, fmt.Errorf("-players must be %d..%d", room.MinPlayers, room.MaxPlayers)
	}
	if mode == "host" && (o.ttl <= 0 || o.ttl > room.MaxTTL) {
		return o, errors.New("-ttl must be in (0,72h]")
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["dir"] {
		if set["profile"] || o.dir == "" {
			return o, errors.New("specify a nonempty -dir or a -profile, not both")
		}
		o.dir, err = filepath.Abs(o.dir)
	} else {
		o.dir, err = home.ProfileDir(root, *profile)
	}
	return o, err
}
