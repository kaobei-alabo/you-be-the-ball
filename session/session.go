// Package session exchanges bounded request/reply frames on an authenticated, reliable stream.
package session

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"time"

	"you-be-the-ball.local/core/room"
)

// Protocol stays unchanged so previously distributed clients can join the renamed CLI.
const Protocol = "/p2p-core/room/1.0.0"
const maxFrame = 256 << 10
const timeout = 5 * time.Second

type Stream interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

type request struct {
	Op               string
	Room, Secret, ID string `json:",omitempty"`
	After            int    `json:",omitempty"`
	Body             []byte `json:",omitempty"`
}

type response struct {
	Err     string     `json:",omitempty"`
	Page    *room.Page `json:",omitempty"`
	Listing *room.Info `json:",omitempty"`
	Seq     int        `json:",omitempty"`
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
	n, err := w.Write(buf)
	if err == nil && n != len(buf) {
		return io.ErrShortWrite
	}
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

func exchange(s Stream, q request) (response, error) {
	var res response
	if err := s.SetDeadline(time.Now().Add(timeout)); err != nil {
		return res, err
	}
	if err := send(s, q); err != nil {
		return res, err
	}
	err := recv(s, &res)
	return res, err
}

// Serve owns the stream. authenticatedPeer must be taken from the secure transport.
func Serve(s Stream, r *room.Room, authenticatedPeer string) {
	defer s.Close()
	var seat *room.Seat
	defer func() {
		if seat != nil {
			seat.Leave()
		}
	}()
	for {
		if err := s.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
			return
		}
		var q request
		if err := recv(s, &q); err != nil {
			return
		}
		var res response
		var err error
		done := false
		switch {
		case seat == nil && q.Op == "list":
			var info room.Info
			info, err = r.Listing()
			if err == nil {
				res.Listing = &info
			}
			done = true
		case seat == nil && q.Op == "join":
			seat, err = r.Join(authenticatedPeer, q.Room, q.Secret)
			if err == nil {
				var p room.Page
				p, err = seat.Since(q.After)
				res.Page = &p
			}
			done = err != nil
		case seat != nil && q.Op == "sync":
			var p room.Page
			p, err = seat.Since(q.After)
			res.Page = &p
			done = err != nil
		case seat != nil && q.Op == "submit":
			res.Seq, err = seat.Submit(q.ID, q.Body)
		default:
			err = errors.New("operation not allowed")
			done = true
		}
		if err != nil {
			res = response{Err: err.Error()}
		}
		if err := send(s, res); err != nil || done {
			return
		}
	}
}

// Listing does not join the room or reserve a seat. It owns and closes s.
func Listing(s Stream) (room.Info, error) {
	defer s.Close()
	res, err := exchange(s, request{Op: "list"})
	if err != nil {
		return room.Info{}, err
	}
	if res.Err != "" {
		return room.Info{}, errors.New(res.Err)
	}
	if res.Listing == nil {
		return room.Info{}, errors.New("missing listing")
	}
	return *res.Listing, nil
}
