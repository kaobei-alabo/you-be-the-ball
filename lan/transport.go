package lan

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// A replication address is "<peerID>/<roomID>/<nonce>": the raft ServerID and
// ServerAddress of one run of one player's program. Each address is served on
// its own protocol ID, so a restarted program (new nonce, empty memory) can
// never be mistaken for the run that held the old address's votes and log.
func address(p peer.ID, roomID, nonce string) string { return p.String() + "/" + roomID + "/" + nonce }

func parseAddress(a string) (peer.ID, protocol.ID, error) {
	parts := strings.Split(a, "/")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", "", errors.New("invalid replication address")
	}
	p, err := peer.Decode(parts[0])
	return p, protocol.ID(raftProto + parts[1] + "/" + parts[2]), err
}

// hostOf is the peer that r currently follows or is, if r knows one.
func hostOf(r *raft.Raft) (peer.ID, bool) {
	a, _ := r.LeaderWithID()
	p, _, err := parseAddress(string(a))
	return p, err == nil
}

type addr string

func (a addr) Network() string { return "libp2p" }
func (a addr) String() string  { return string(a) }

type conn struct{ network.Stream }

func (c conn) LocalAddr() net.Addr  { return addr(c.Conn().LocalPeer().String()) }
func (c conn) RemoteAddr() net.Addr { return addr(c.Conn().RemotePeer().String()) }
func (c conn) Close() error {
	if err := c.Stream.Close(); err != nil {
		_ = c.Stream.Reset()
		return err
	}
	return nil
}

// layer is a raft.StreamLayer over libp2p streams. It accepts streams only from
// roster members.
type layer struct {
	n      *Node
	self   string
	proto  protocol.ID
	roster map[peer.ID]bool
	ch     chan net.Conn
	done   chan struct{}
	once   sync.Once
}

func newLayer(n *Node, self string, roster map[peer.ID]bool) (*layer, error) {
	_, proto, err := parseAddress(self)
	if err != nil {
		return nil, err
	}
	l := &layer{n: n, self: self, proto: proto, roster: roster, ch: make(chan net.Conn), done: make(chan struct{})}
	n.h.SetStreamHandler(proto, func(s network.Stream) {
		if !l.roster[s.Conn().RemotePeer()] {
			_ = s.Reset()
			return
		}
		select {
		case l.ch <- conn{s}:
		case <-l.done:
			_ = s.Reset()
		}
	})
	return l, nil
}

func (l *layer) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, errors.New("replication closed")
	}
}

func (l *layer) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.n.h.RemoveStreamHandler(l.proto)
	})
	return nil
}

// Addr must equal this node's raft ServerAddress.
func (l *layer) Addr() net.Addr { return addr(l.self) }

func (l *layer) Dial(a raft.ServerAddress, d time.Duration) (net.Conn, error) {
	p, proto, err := parseAddress(string(a))
	if err != nil {
		return nil, err
	}
	// Keep trying until d: raft backs off longer after every failed dial (up to
	// about 10 s), and so does libp2p, so failing at once while a link is down
	// would leave a player that is back online lagging for that long. Short
	// attempts, because a dial to a peer that just refused one can hang.
	end := time.Now().Add(d)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), min(time.Until(end), 300*time.Millisecond))
		s, err := l.n.h.NewStream(ctx, p, proto)
		cancel()
		if err == nil {
			return conn{s}, nil
		}
		if !time.Now().Before(end) {
			return nil, err
		}
		l.n.forget(p)
		select {
		case <-l.done:
			return nil, err
		case <-time.After(50 * time.Millisecond):
		}
	}
}
