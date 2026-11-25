package lan

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

const (
	lobbyProto = protocol.ID("/you-be-the-ball/room/2")
	oldProto   = protocol.ID("/p2p-core/room/1.0.0") // answered only with "please upgrade"
	raftProto  = "/you-be-the-ball/raft/1/"          // + <roomID>/<nonce>
	maxFrame   = 256 << 10
	timeout    = 5 * time.Second
)

// request is what a peer asks. The actor is always the stream's authenticated
// remote peer, never a field here.
type request struct {
	Op               string
	Room, Secret, ID string   `json:",omitempty"`
	Node             string   `json:",omitempty"` // join: the guest's replication address
	Addrs            []string `json:",omitempty"` // join: the guest's listen addresses
	Body             []byte   `json:",omitempty"`
	Seq              int      `json:",omitempty"` // closed: the host's version
}

type reply struct {
	Err      string          `json:",omitempty"`
	Lobby    *room.LobbyInfo `json:",omitempty"`
	Setup    *room.Setup     `json:",omitempty"`
	Seq      int             `json:",omitempty"`
	Redirect string          `json:",omitempty"` // peer ID of the host
	Retry    bool            `json:",omitempty"`
}

func send(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > maxFrame {
		return errors.New("frame too large")
	}
	buf := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(buf, uint32(len(b)))
	copy(buf[4:], b)
	_, err = w.Write(buf)
	return err
}

func recv(r io.Reader, v any) error {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > maxFrame {
		return errors.New("invalid frame size")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// ask sends one request on a new stream and reads one reply.
func ask(ctx context.Context, n *Node, to peer.ID, q request) (reply, error) {
	var r reply
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s, err := n.h.NewStream(ctx, to, lobbyProto)
	if err != nil {
		return r, err
	}
	defer s.Close()
	dl, _ := ctx.Deadline()
	if err = s.SetDeadline(dl); err == nil {
		err = send(s, q)
	}
	if err == nil {
		err = recv(s, &r)
	}
	return r, err
}

// serve answers the lobby protocol for the rooms on this node.
func (n *Node) serve(s network.Stream) {
	var q request
	if s.SetDeadline(time.Now().Add(timeout)) != nil || recv(s, &q) != nil {
		_ = s.Reset()
		return
	}
	from := s.Conn().RemotePeer()
	var r reply
	switch p := n.room(q.Room); {
	case q.Op == "list":
		info, err := n.listing()
		r = reply{Lobby: &info}
		if err != nil {
			r = reply{Err: err.Error()}
		}
	case q.Op == "join" && p != nil:
		p.admit(s, from, q) // owns the stream from here on
		return
	case q.Op == "submit" && p != nil:
		r = p.serveSubmit(from, q)
	case q.Op == "closed" && p != nil:
		r = p.serveClosed(from, q)
	case q.Op == "join":
		r = reply{Err: room.ErrInvite.Error()}
	default:
		r = reply{Err: "no such room or operation"}
	}
	_ = s.SetWriteDeadline(time.Now().Add(timeout))
	_ = send(s, r)
	_ = s.Close()
}

// upgrade answers clients of the old protocol, which print Err.
func upgrade(s network.Stream) {
	_ = s.SetDeadline(time.Now().Add(timeout))
	var q json.RawMessage
	_ = recv(s, &q)
	_ = send(s, reply{Err: "this room needs a newer you-be-the-ball; please upgrade"})
	_ = s.Close()
}
