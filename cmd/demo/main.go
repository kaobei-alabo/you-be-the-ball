// Command demo plays one counter game between three players in this process,
// over libp2p on 127.0.0.1, including a host change and a retried move.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/lan"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

var (
	names  = []string{"Alice", "Bob", "Carol"}
	timing = lan.Timing{Heartbeat: 100 * time.Millisecond, Lease: 50 * time.Millisecond, Commit: 2 * time.Millisecond}
)

// waitFor polls until ok holds, for up to 20 s.
func waitFor(what string, ok func() bool) error {
	for deadline := time.Now().Add(20 * time.Second); !ok(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			return errors.New("timed out waiting until " + what)
		}
	}
	return nil
}

func run() error {
	tmp, err := os.MkdirTemp("", "you-be-the-ball-demo-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var nodes []*lan.Node
	var ps []*lan.Player
	defer func() {
		for _, p := range ps {
			p.Close()
		}
		for _, n := range nodes {
			n.Close()
		}
	}()
	for _, name := range names {
		n, err := lan.New(filepath.Join(tmp, name), "/ip4/127.0.0.1/tcp/0")
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}
	opts := func(i int) lan.Options { return lan.Options{Dir: filepath.Join(tmp, names[i]), Timing: timing} }
	alice, err := lan.Host(nodes[0], counter.Game{}, room.Config{Players: 3}, opts(0))
	if err != nil {
		return err
	}
	ps = append(ps, alice)
	inv, err := alice.Invite()
	if err != nil {
		return err
	}
	for i := 1; i < 3; i++ {
		p, err := lan.Join(ctx, nodes[i], counter.Game{}, inv, opts(i))
		if err != nil {
			return fmt.Errorf("%s joins: %w", names[i], err)
		}
		ps = append(ps, p)
	}
	bob, carol := ps[1], ps[2]
	fmt.Println("Lobby: Alice hosts; Bob and Carol joined.")
	if err := alice.Start(ctx); err != nil {
		return err
	}
	fmt.Println("Alice started the game.")

	seq, err := bob.Submit(ctx, "bob-1", []byte("inc"))
	if err != nil || seq != 2 {
		return fmt.Errorf("bob-1: v%d, %v", seq, err)
	}
	fmt.Printf("Bob incremented: v%d.\n", seq)

	nodes[0].SetOffline(true)
	fmt.Println("Alice is cut off from the network.")
	newHost := -1
	if err := waitFor("Bob or Carol hosts", func() bool {
		for i, p := range ps[1:] {
			if p.Status().IsHost {
				newHost = i + 1
				return true
			}
		}
		return false
	}); err != nil {
		return err
	}
	fmt.Printf("%s took over as host.\n", names[newHost])

	if seq, err = carol.Submit(ctx, "carol-1", []byte("inc")); err != nil || seq != 3 {
		return fmt.Errorf("carol-1: v%d, %v", seq, err)
	}
	fmt.Printf("Carol incremented: v%d.\n", seq)
	if seq, err = bob.Submit(ctx, "bob-1", []byte("inc")); err != nil || seq != 2 {
		return fmt.Errorf("bob-1 retry: v%d, %v", seq, err)
	}
	fmt.Printf("Bob retried bob-1: original receipt v%d.\n", seq)

	nodes[0].SetOffline(false)
	if err := waitFor("Alice catches up", func() bool { return alice.Status().Version == 3 }); err != nil {
		return err
	}
	fmt.Println("Alice is back online and caught up.")
	if err := ps[newHost].Finish(ctx); err != nil {
		return err
	}
	fmt.Printf("%s finished the game.\n", names[newHost])
	fmt.Println("Each player saved a record under", tmp, "(removed when the demo exits):")
	for i, p := range ps {
		if err := waitFor(names[i]+" saves a record", func() bool { return p.Status().Record != "" }); err != nil {
			return err
		}
		s := p.Status()
		if s.Phase != room.Finished || s.Version != 4 || string(p.State()) != "2" {
			return fmt.Errorf("%s: %s v%d counter=%s", names[i], s.Phase, s.Version, p.State())
		}
		rel, _ := filepath.Rel(tmp, s.Record)
		fmt.Printf("  %s: %s\n", names[i], rel)
	}
	fmt.Printf("All players: counter=2 v4 finished; host changed from Alice to %s\n", names[newHost])
	return nil
}
