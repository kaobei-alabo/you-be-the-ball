// Command demo exercises the room protocol over net.Pipe without a socket or external dependency.
package main

import (
	"fmt"
	"net"
	"os"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
	"github.com/kaobei-alabo/you-be-the-ball/session"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	r, err := room.Open("", room.Config{Host: "alice", Players: 2}, counter.Game{})
	if err != nil {
		return err
	}
	defer r.Close()
	id, _, _ := r.Invite()
	c := session.NewClient(counter.Game{}, id, "alice", "")
	defer c.Close()
	connect := func() error { a, b := net.Pipe(); go session.Serve(b, r, "bob"); return c.Attach(a) }
	if err := connect(); err != nil {
		return err
	}
	fmt.Println("Lobby: Alice and Bob joined; waiting for Alice to start.")
	if err := r.Start(); err != nil {
		return err
	}
	seq, err := c.Submit("bob-1", []byte("inc"))
	if err != nil {
		return err
	}
	if err = c.Sync(); err != nil {
		return err
	}
	fmt.Printf("Bob incremented: v%d. Bob disconnects.\n", seq)
	c.Disconnect()
	if _, err := r.Submit("alice-1", []byte("inc")); err != nil {
		return err
	}
	fmt.Println("Alice incremented while Bob was offline.")
	if err = connect(); err != nil {
		return err
	}
	retry, err := c.Submit("bob-1", []byte("inc"))
	if err != nil {
		return err
	}
	if err = c.Sync(); err != nil {
		return err
	}
	a, x, _ := r.View()
	b, y, _ := c.View()
	fmt.Printf("Retry of bob-1: original receipt v%d.\nHost: counter=%s v%d; guest: counter=%s v%d\n", retry, x, a.Version, y, b.Version)
	if seq != retry || a.Version != 3 || b.Version != 3 || string(x) != "2" || string(y) != "2" {
		return fmt.Errorf("replicas did not converge")
	}
	return nil
}
