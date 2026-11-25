// Package lan binds the transport-neutral room protocol to authenticated libp2p streams.
package lan

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	"you-be-the-ball.local/core/room"
	"you-be-the-ball.local/core/session"
)

// Keep the v1 discovery name interoperable with pre-rename builds.
const service = "_p2pcore-v1._udp"

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

func New(dir, listen string) (host.Host, error) {
	key, err := identity(dir)
	if err != nil {
		return nil, err
	}
	return libp2p.New(libp2p.Identity(key), libp2p.ListenAddrStrings(listen), libp2p.DisableRelay())
}

func Serve(h host.Host, r *room.Room) {
	h.SetStreamHandler(session.Protocol, func(s network.Stream) { session.Serve(s, r, s.Conn().RemotePeer().String()) })
}

func Invitation(h host.Host, r *room.Room) (Invite, error) {
	id, secret, err := r.Invite()
	if err != nil {
		return Invite{}, err
	}
	// Use local listening interfaces, not addresses chosen for public advertisement.
	addrs, err := h.Network().InterfaceListenAddresses()
	if err != nil {
		return Invite{}, err
	}
	if len(addrs) == 0 {
		return Invite{}, errors.New("no listening addresses")
	}
	return Invite{Room: id, Host: peer.AddrInfo{ID: h.ID(), Addrs: addrs}, Secret: secret}, nil
}

func Dial(ctx context.Context, h host.Host, p peer.AddrInfo) (network.Stream, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := h.Connect(ctx, p); err != nil {
		return nil, err
	}
	return h.NewStream(ctx, p.ID, session.Protocol)
}

type notify func(peer.AddrInfo)

func (f notify) HandlePeerFound(p peer.AddrInfo) { f(p) }

// Advertise announces the peer, not its room, roster, or invite secret.
func Advertise(h host.Host) (io.Closer, error) {
	srv := mdns.NewMdnsService(h, service, notify(func(peer.AddrInfo) {}))
	if err := srv.Start(); err != nil {
		srv.Close()
		return nil, err
	}
	return srv, nil
}

// Browse asks nearby nodes for a public lobby, without admitting the browsing peer.
func Browse(ctx context.Context, h host.Host, found func(Invite, room.Info)) error {
	peers := make(chan peer.AddrInfo, 32)
	srv := mdns.NewMdnsService(h, service, notify(func(p peer.AddrInfo) {
		if p.ID != h.ID() {
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
			s, err := Dial(ctx, h, p)
			if err != nil {
				continue
			}
			info, err := session.Listing(s)
			if err != nil || info.Host != p.ID.String() {
				continue
			}
			seen[p.ID] = true
			found(Invite{Room: info.ID, Host: p}, info)
		}
	}
}

func Join(ctx context.Context, h host.Host, i Invite, c *session.Client) error {
	s, err := Dial(ctx, h, i.Host)
	if err != nil {
		return fmt.Errorf("dial room host: %w", err)
	}
	return c.Attach(s)
}
