package lan

import (
	"context"
	"testing"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
	"github.com/kaobei-alabo/you-be-the-ball/session"
)

// Uses real encrypted libp2p TCP streams, not net.Pipe or a mocked peer ID.
func TestLoopbackCounter(t *testing.T) {
	const addr = "/ip4/127.0.0.1/tcp/0"
	hostDir := t.TempDir()
	guestDir := t.TempDir()
	h, err := New(hostDir, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	g, err := New(guestDir, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	r, err := room.Open(hostDir, room.Config{Host: h.ID().String(), Players: 2}, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	Serve(h, r)
	inv, err := Invitation(h, r)
	if err != nil {
		t.Fatal(err)
	}
	text, err := inv.Encode()
	if err != nil {
		t.Fatal(err)
	}
	inv, err = Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s, err := Dial(ctx, g, inv.Host)
	if err != nil {
		t.Fatal(err)
	}
	info, err := session.Listing(s)
	if err != nil || info.ID != inv.Room {
		t.Fatal(info, err)
	}
	if r.Start() == nil {
		t.Fatal("browsing reserved a seat")
	}
	c := session.NewClient(counter.Game{}, inv.Room, inv.Host.ID.String(), inv.Secret)
	defer c.Close()
	if err := Join(ctx, g, inv, c); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	seq, err := c.Submit("g1", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	c.Disconnect()
	if err := g.Network().ClosePeer(h.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit("h1", []byte("inc")); err != nil {
		t.Fatal(err)
	}
	if err := Join(ctx, g, inv, c); err != nil {
		t.Fatal(err)
	}
	retry, err := c.Submit("g1", []byte("inc"))
	if err != nil || retry != seq {
		t.Fatal(retry, err)
	}
	if err = c.Sync(); err != nil {
		t.Fatal(err)
	}
	hi, x, err := r.View()
	if err != nil {
		t.Fatal(err)
	}
	gi, y, err := c.View()
	if err != nil || hi.Version != 3 || hi.Version != gi.Version || string(x) != "2" || string(y) != "2" {
		t.Fatalf("host=%d/%s guest=%d/%s %v", hi.Version, x, gi.Version, y, err)
	}
}
