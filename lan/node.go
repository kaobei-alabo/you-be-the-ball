// Package lan plays a room over libp2p: the lobby protocol before the start,
// Raft replication of every player's copy after it, and the API a front end uses.
package lan

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// A new name, so builds that speak the old protocol do not list new rooms.
const service = "_ybtb-v2._udp"

type Invite struct {
	Room   string
	Host   peer.AddrInfo
	Secret string `json:",omitempty"`
}

func (i Invite) Encode() (string, error) {
	b, err := json.Marshal(&i)
	return base64.RawURLEncoding.EncodeToString(b), err
}

func Parse(text string) (Invite, error) {
	var i Invite
	if len(text) > 16384 {
		return i, errors.New("invite too large")
	}
	b, err := base64.RawURLEncoding.DecodeString(text)
	if err == nil {
		err = json.Unmarshal(b, &i)
	}
	if err != nil || i.Room == "" || i.Host.ID == "" || len(i.Host.Addrs) == 0 {
		return Invite{}, errors.New("invalid invite; paste the entire token")
	}
	return i, nil
}

func identity(dir string) (crypto.PrivKey, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "identity.key")
	raw, err := os.ReadFile(path)
	if err == nil {
		return crypto.UnmarshalPrivateKey(raw)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	raw, err = crypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, ".key-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(raw)
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err = os.Link(tmp.Name(), path); err != nil {
		return nil, err
	}
	return key, nil
}

// Node is this program's libp2p host. It serves the lobby protocol for every
// Player made from it.
type Node struct {
	h     host.Host // unexported: front ends must not replace the protocol handlers
	gate  *gate
	mu    sync.Mutex
	rooms map[string]*Player
}

func New(dir, listen string) (*Node, error) {
	key, err := identity(dir)
	if err != nil {
		return nil, err
	}
	g := &gate{blocked: make(map[peer.ID]bool)}
	h, err := libp2p.New(libp2p.Identity(key), libp2p.ListenAddrStrings(listen), libp2p.DisableRelay(), libp2p.ConnectionGater(g))
	if err != nil {
		return nil, err
	}
	n := &Node{h: h, gate: g, rooms: make(map[string]*Player)}
	h.SetStreamHandler(lobbyProto, n.serve)
	h.SetStreamHandler(oldProto, upgrade)
	return n, nil
}

func (n *Node) ID() peer.ID  { return n.h.ID() }
func (n *Node) Close() error { return n.h.Close() }

// SetOffline refuses and closes every connection while on, as if the network
// cable were pulled, without stopping the program.
func (n *Node) SetOffline(on bool) {
	n.gate.mu.Lock()
	n.gate.offline = on
	n.gate.mu.Unlock()
	n.reset(n.h.Network().Peers()...)
}

// block cuts this node off from one peer (tests).
func (n *Node) block(p peer.ID, on bool) {
	n.gate.mu.Lock()
	n.gate.blocked[p] = on
	n.gate.mu.Unlock()
	n.reset(p)
}

// reset drops connections and forgets failed dials, so a healed link is used at once.
func (n *Node) reset(peers ...peer.ID) {
	for _, p := range peers {
		_ = n.h.Network().ClosePeer(p)
	}
	n.forget(n.h.Peerstore().Peers()...)
}

// forget clears libp2p's dial backoff (5 s or more), which would otherwise keep
// a peer that is back online unreachable for that long.
func (n *Node) forget(peers ...peer.ID) {
	if s, ok := n.h.Network().(*swarm.Swarm); ok {
		for _, p := range peers {
			s.Backoff().Clear(p)
		}
	}
}

func (n *Node) addrs() ([]ma.Multiaddr, error) {
	// Local interfaces, not addresses chosen for public advertisement.
	addrs, err := n.h.Network().InterfaceListenAddresses()
	if err == nil && len(addrs) == 0 {
		err = errors.New("no listening addresses")
	}
	return addrs, err
}

func (n *Node) add(p *Player) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.rooms[p.id] != nil {
		return errors.New("already in this room")
	}
	n.rooms[p.id] = p
	return nil
}

func (n *Node) remove(p *Player) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.rooms[p.id] == p {
		delete(n.rooms, p.id)
	}
}

func (n *Node) room(id string) *Player {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rooms[id]
}

func (n *Node) listing() (room.LobbyInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.rooms {
		if p.lobby != nil {
			if info, err := p.lobby.Listing(); err == nil {
				return info, nil
			}
		}
	}
	return room.LobbyInfo{}, errors.New("no open public room")
}

type gate struct {
	mu      sync.Mutex
	offline bool
	blocked map[peer.ID]bool
}

func (g *gate) allow(p peer.ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.offline && !g.blocked[p]
}

func (g *gate) InterceptPeerDial(p peer.ID) bool                 { return g.allow(p) }
func (g *gate) InterceptAddrDial(p peer.ID, _ ma.Multiaddr) bool { return g.allow(p) }
func (g *gate) InterceptAccept(network.ConnMultiaddrs) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.offline
}
func (g *gate) InterceptSecured(_ network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	return g.allow(p)
}
func (g *gate) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }

type notify func(peer.AddrInfo)

func (f notify) HandlePeerFound(p peer.AddrInfo) { f(p) }

// Advertise announces the peer, not its room, roster, or invite secret.
func Advertise(n *Node) (io.Closer, error) {
	srv := mdns.NewMdnsService(n.h, service, notify(func(peer.AddrInfo) {}))
	if err := srv.Start(); err != nil {
		srv.Close()
		return nil, err
	}
	return srv, nil
}

// Browse asks nearby nodes for a public lobby with a free seat. It takes no seat.
func Browse(ctx context.Context, n *Node, found func(Invite, room.LobbyInfo)) error {
	peers := make(chan peer.AddrInfo, 32)
	srv := mdns.NewMdnsService(n.h, service, notify(func(p peer.AddrInfo) {
		if p.ID != n.ID() {
			select {
			case peers <- p:
			default:
			}
		}
	}))
	if err := srv.Start(); err != nil {
		srv.Close()
		return err
	}
	defer srv.Close()
	seen := make(map[peer.ID]bool)
	for {
		select {
		case <-ctx.Done():
			return nil
		case p := <-peers:
			if seen[p.ID] {
				continue
			}
			info, err := list(ctx, n, p)
			if err != nil || info.Host != p.ID.String() {
				continue
			}
			seen[p.ID] = true
			found(Invite{Room: info.ID, Host: p}, info)
		}
	}
}

// list asks one node for its public lobby.
func list(ctx context.Context, n *Node, p peer.AddrInfo) (room.LobbyInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := n.h.Connect(ctx, p); err != nil {
		return room.LobbyInfo{}, err
	}
	r, err := ask(ctx, n, p.ID, request{Op: "list"})
	if err == nil && r.Err != "" {
		err = errors.New(r.Err)
	} else if err == nil && r.Lobby == nil {
		err = errors.New("missing listing")
	}
	if err != nil {
		return room.LobbyInfo{}, err
	}
	return *r.Lobby, nil
}
