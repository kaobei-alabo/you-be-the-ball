package lan

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func TestLobbyOverTheNetwork(t *testing.T) {
	ctx := context.Background()
	o := Options{Timing: fast}
	h := hostRoom(t, room.Config{Players: 3}, o)
	inv, err := h.Invite()
	if err != nil {
		t.Fatal(err)
	}
	text, _ := inv.Encode()
	if inv, err = Parse(text); err != nil {
		t.Fatal(err)
	}
	stranger := newNode(t, t.TempDir())
	online := func(n int, ps ...*Player) {
		t.Helper()
		waitFor(t, "seats taken", func() bool {
			for _, p := range ps {
				if len(p.Status().Online) != n {
					return false
				}
			}
			return true
		})
	}

	info, err := list(ctx, stranger, inv.Host)
	if err != nil || info.ID != inv.Room || len(info.Online) != 1 || len(h.Status().Online) != 1 {
		t.Fatal("listing", info, err)
	}
	for want, q := range map[string]request{
		"invalid replication address": {Op: "join", Room: inv.Room, Node: address(h.n.ID(), inv.Room, "x")},
		"too many addresses":          {Op: "join", Room: inv.Room, Node: address(stranger.ID(), inv.Room, "x"), Addrs: make([]string, 17)},
	} {
		if r, err := ask(ctx, stranger, h.n.ID(), q); err != nil || r.Err != want {
			t.Fatal("forged join:", r, err)
		}
	}
	for _, bad := range []Invite{{Room: inv.Room, Host: inv.Host, Secret: "nope"}, {Room: "nope", Host: inv.Host}} {
		if _, err := join(t, newNode(t, t.TempDir()), bad, o); err == nil || !strings.Contains(err.Error(), "invalid room invite") {
			t.Fatal("bad invite admitted:", err)
		}
	}
	a, err := join(t, newNode(t, t.TempDir()), inv, o)
	if err != nil {
		t.Fatal(err)
	}
	online(2, h, a)
	if _, err := join(t, stranger, Invite{Room: inv.Room, Host: peer.AddrInfo{ID: a.n.ID(), Addrs: a.n.h.Addrs()}}, o); err == nil {
		t.Fatal("a guest's node seated someone")
	}
	if a.Start(ctx) == nil {
		t.Fatal("a guest started the game")
	}
	b, err := join(t, newNode(t, t.TempDir()), inv, o)
	if err != nil {
		t.Fatal(err)
	}
	online(3, h, a, b)
	if _, err := list(ctx, stranger, inv.Host); err == nil {
		t.Fatal("a full room is listed")
	}
	cNode := newNode(t, t.TempDir())
	if _, err := join(t, cNode, inv, o); err == nil || !strings.Contains(err.Error(), "room full") {
		t.Fatal("joined a full room:", err)
	}
	b.Close()
	online(2, h, a)
	if _, err := list(ctx, stranger, inv.Host); err != nil {
		t.Fatal("free seat not listed:", err)
	}
	c, err := join(t, cNode, inv, o)
	if err != nil {
		t.Fatal(err)
	}
	online(3, h, a, c)
	if err := h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := join(t, newNode(t, t.TempDir()), inv, o); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatal("joined a started game:", err)
	}

	// A private lobby is never listed and needs its secret.
	priv := hostRoom(t, room.Config{Players: 2, Private: true}, o)
	pinv, _ := priv.Invite()
	if _, err := list(ctx, stranger, pinv.Host); err == nil {
		t.Fatal("a private room is listed")
	}
	if _, err := join(t, stranger, Invite{Room: pinv.Room, Host: pinv.Host}, o); err == nil {
		t.Fatal("joined a private room without its secret")
	}
	if _, err := join(t, stranger, pinv, o); err != nil {
		t.Fatal(err)
	}
}

func TestOldBuildsAreToldToUpgrade(t *testing.T) {
	a, b := newNode(t, t.TempDir()), newNode(t, t.TempDir())
	b.h.Peerstore().AddAddrs(a.ID(), a.h.Addrs(), peerstore.PermanentAddrTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := b.h.NewStream(ctx, a.ID(), oldProto)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var r reply
	if err := send(s, map[string]string{"Op": "list"}); err != nil {
		t.Fatal(err)
	}
	if err := recv(s, &r); err != nil || !strings.Contains(r.Err, "please upgrade") {
		t.Fatal(r, err)
	}

	// And the other way round: a host that does not speak the new protocol.
	a.h.RemoveStreamHandler(lobbyProto)
	_ = b.h.Peerstore().RemoveProtocols(a.ID(), lobbyProto) // as if never advertised
	_, err = join(t, b, Invite{Room: "r", Host: peer.AddrInfo{ID: a.ID(), Addrs: a.h.Addrs()}}, Options{})
	if err == nil || !strings.Contains(err.Error(), "older you-be-the-ball") {
		t.Fatal(err)
	}
}

func TestStrangersAndStaleRunsAreShutOut(t *testing.T) {
	dir := t.TempDir()
	nodes := []*Node{newNode(t, t.TempDir()), newNode(t, t.TempDir()), newNode(t, dir)}
	ps := playOn(t, nodes, room.Config{Players: 3}, fast)
	h := ps[0]
	inc(t, ps[1], "m1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stranger := newNode(t, t.TempDir())
	stranger.h.Peerstore().AddAddrs(h.n.ID(), h.n.h.Addrs(), peerstore.PermanentAddrTTL)
	_, proto, _ := parseAddress(h.self)
	if !served(ctx, ps[1].n, h.n.ID(), proto) {
		t.Fatal("a member cannot reach the replication protocol")
	}
	if served(ctx, stranger, h.n.ID(), proto) {
		t.Fatal("a stranger reached the replication protocol")
	}
	r, err := ask(ctx, stranger, h.n.ID(), request{Op: "submit", Room: h.id, ID: "x", Body: []byte("inc")})
	if err != nil || !strings.Contains(r.Err, "not a player") {
		t.Fatal("stranger's move:", r, err)
	}

	// The same identity, run again in the same room, gets a new replication
	// address; the old one is not served.
	old := ps[2]
	crash(old)
	n := newNode(t, dir)
	if again := newPlayer(n, old.g, Options{}, old.id); n.ID() != old.n.ID() || again.self == old.self {
		t.Fatal("a second run reuses the replication address", old.self)
	}
	h.n.h.Peerstore().AddAddrs(n.ID(), n.h.Addrs(), peerstore.PermanentAddrTTL)
	_, stale, _ := parseAddress(old.self)
	if served(ctx, h.n, n.ID(), stale) {
		t.Fatal("a stale replication address is still served")
	}
	if seq := inc(t, ps[1], "m2"); seq != 3 {
		t.Fatal("seq", seq)
	}
	if string(h.State()) != "2" {
		t.Fatal("state", string(h.State()))
	}
}

// served reports whether a stream on proto lives through one exchange. libp2p
// may negotiate lazily, so NewStream alone proves nothing.
func served(ctx context.Context, n *Node, to peer.ID, proto protocol.ID) bool {
	s, err := n.h.NewStream(ctx, to, proto)
	if err != nil {
		return false
	}
	defer s.Reset()
	_ = s.SetDeadline(time.Now().Add(time.Second))
	if _, err := s.Write([]byte{0}); err != nil {
		return false
	}
	_, err = s.Read(make([]byte, 1))
	return err == nil || os.IsTimeout(err)
}

func TestDialWaitsForAPeerComingBack(t *testing.T) {
	// Failing at once would make raft back off for up to 10 s.
	a, b := newNode(t, t.TempDir()), newNode(t, t.TempDir())
	a.h.Peerstore().AddAddrs(b.ID(), b.h.Addrs(), peerstore.PermanentAddrTTL)
	la, err := newLayer(a, address(a.ID(), "r", "x"), map[peer.ID]bool{b.ID(): true})
	if err != nil {
		t.Fatal(err)
	}
	defer la.Close()
	self := address(b.ID(), "r", "y")
	lb, err := newLayer(b, self, map[peer.ID]bool{a.ID(): true})
	if err != nil {
		t.Fatal(err)
	}
	defer lb.Close()
	b.SetOffline(true)
	time.AfterFunc(time.Second, func() { b.SetOffline(false) })
	c, err := la.Dial(raft.ServerAddress(self), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}
