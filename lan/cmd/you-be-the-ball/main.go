package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"you-be-the-ball.local/core/example/counter"
	"you-be-the-ball.local/core/room"
	"you-be-the-ball.local/core/session"
	"you-be-the-ball.local/lan"
	"you-be-the-ball.local/lan/internal/apphome"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const usage = `you-be-the-ball host|find|join [flags] [invite]
  you-be-the-ball migrate OLD_PROFILE_DIR [OLD_PROFILE_DIR ...]
Defaults: ~/.you-be-the-ball/config.json and ~/.you-be-the-ball/{host,join,find}/`

type options struct {
	dir, listen, token string
	players            int
	ttl, wait          time.Duration
	private, mdns      bool
}

func parseOptions(root, mode string, args []string) (options, error) {
	var o options
	cfg, err := apphome.LoadConfig(root)
	if err != nil {
		return o, err
	}
	addr := cfg.PeerListen
	if mode == "host" {
		addr = cfg.HostListen
	}
	fs := flag.NewFlagSet("you-be-the-ball "+mode, flag.ContinueOnError)
	profile := fs.String("profile", mode, "profile name under ~/.you-be-the-ball (one per running player)")
	fs.StringVar(&o.dir, "dir", "", "explicit data directory instead of a home profile (legacy override)")
	fs.StringVar(&o.listen, "listen", addr, "libp2p listen multiaddress")
	fs.IntVar(&o.players, "players", cfg.Players, "players required, including host (new room)")
	fs.DurationVar(&o.ttl, "ttl", room.MaxTTL, "room lifetime (new room)")
	fs.BoolVar(&o.private, "private", cfg.Private, "require invite secret; hide lobby (new room)")
	fs.BoolVar(&o.mdns, "mdns", cfg.MDNS, "advertise the hosting peer via multicast DNS")
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
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["dir"] {
		if set["profile"] || o.dir == "" {
			return o, errors.New("specify a nonempty -dir or a -profile, not both")
		}
		o.dir, err = filepath.Abs(o.dir)
	} else {
		o.dir, err = apphome.ProfileDir(root, *profile)
	}
	return o, err
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	mode := args[0]
	switch mode {
	case "help", "-h", "--help":
		fmt.Println(usage)
		return nil
	case "host", "find", "join", "migrate":
	default:
		return errors.New(usage)
	}
	root, err := apphome.Root()
	if err != nil {
		return err
	}
	if mode == "migrate" {
		if len(args) < 2 {
			return errors.New("usage: you-be-the-ball migrate OLD_PROFILE_DIR [OLD_PROFILE_DIR ...] (stop old processes first)")
		}
		for _, src := range args[1:] {
			dest, err := apphome.Migrate(root, src)
			if err != nil {
				return err
			}
			fmt.Printf("Moved %s to %s\n", src, dest)
		}
		return nil
	}
	o, err := parseOptions(root, mode, args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	var invite lan.Invite
	if mode == "join" {
		invite, err = lan.Parse(o.token)
		if err != nil {
			return err
		}
	}
	fmt.Fprintln(os.Stderr, "Data directory:", o.dir)
	h, err := lan.New(o.dir, o.listen)
	if err != nil {
		return err
	}
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if mode == "find" {
		bctx, stop := context.WithTimeout(ctx, o.wait)
		defer stop()
		n := 0
		err := lan.Browse(bctx, h, func(i lan.Invite, info room.Info) {
			if text, e := i.Encode(); e == nil {
				fmt.Printf("%d/%d players; invite: %s\n", len(info.Online), info.Players, text)
				n++
			}
		})
		if err == nil && n == 0 {
			fmt.Println("No open public rooms found. Try the host's invite directly.")
		}
		return err
	}
	var start func() error
	var syncNow, disconnect, reconnect func() error
	var submit func(string, []byte) (int, error)
	var view func() (room.Info, []byte, error)
	if mode == "host" {
		r, err := room.Open(o.dir, room.Config{Host: h.ID().String(), Players: o.players, TTL: o.ttl, Private: o.private}, counter.Game{})
		if err != nil {
			return err
		}
		defer r.Close()
		lan.Serve(h, r)
		i, err := lan.Invitation(h, r)
		if err != nil {
			return err
		}
		text, err := i.Encode()
		if err != nil {
			return err
		}
		fmt.Println("Invite:", text)
		if o.mdns && i.Secret == "" {
			svc, err := lan.Advertise(h)
			if err != nil {
				fmt.Fprintln(os.Stderr, "mDNS unavailable; use invite:", err)
			} else {
				defer svc.Close()
			}
		}
		start = r.Start
		submit = r.Submit
		view = r.View
	} else {
		c := session.NewClient(counter.Game{}, invite.Room, invite.Host.ID.String(), invite.Secret)
		defer c.Close()
		reconnect = func() error { return lan.Join(ctx, h, invite, c) }
		if err := reconnect(); err != nil {
			return err
		}
		disconnect = func() error { c.Disconnect(); return nil }
		syncNow = c.Sync
		submit = c.Submit
		view = c.View
		pctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			tick := time.NewTicker(500 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-pctx.Done():
					return
				case <-tick.C:
					c.Sync()
				}
			}
		}()
		defer func() { stop(); <-done }()
	}
	fmt.Println("Commands: status | start (host) | inc [request-id] | offline / online (guest) | quit")
	scan := bufio.NewScanner(os.Stdin)
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
			if syncNow != nil {
				if e := syncNow(); e != nil {
					fmt.Println("Offline / stale replica:", e)
				}
			}
			var i room.Info
			var state []byte
			i, state, err = view()
			if err == nil {
				fmt.Printf("started=%t online=%d/%d version=%d counter=%s expires=%s\n", i.Started, len(i.Online), i.Players, i.Version, state, i.Expires.Format(time.RFC3339))
			}
		case "start":
			if start == nil {
				err = errors.New("only the host can start")
			} else {
				err = start()
				if err == nil {
					fmt.Println("Started")
				}
			}
		case "inc":
			id := room.Token()
			if len(f) > 1 {
				id = f[1]
			}
			var seq int
			seq, err = submit(id, []byte("inc"))
			if err == nil {
				fmt.Printf("committed %s at v%d\n", id, seq)
			} else {
				fmt.Println("request ID (reuse if retrying):", id)
			}
		case "offline":
			if disconnect != nil {
				err = disconnect()
			} else {
				err = errors.New("guest command")
			}
		case "online":
			if reconnect != nil {
				err = reconnect()
			} else {
				err = errors.New("guest command")
			}
		default:
			err = errors.New("unknown command")
		}
		if err != nil {
			fmt.Println("error:", err)
		}
	}
	return scan.Err()
}
