package session

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"you-be-the-ball.local/core/example/counter"
	"you-be-the-ball.local/core/room"
)

func fixture(t *testing.T) (*room.Room, *Client, string) {
	t.Helper()
	r, err := room.Open("", room.Config{Host: "host", Players: 2}, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	id, _, _ := r.Invite()
	c := NewClient(counter.Game{}, id, "host", "")
	t.Cleanup(c.Close)
	return r, c, id
}

func pipe(r *room.Room, peer string) Stream { a, b := net.Pipe(); go Serve(b, r, peer); return a }
func TestMissedEventsRetryAndPaging(t *testing.T) {
	r, c, _ := fixture(t)
	if _, err := Listing(pipe(r, "guest")); err != nil {
		t.Fatal(err)
	}
	if r.Start() == nil {
		t.Fatal("browsing reserved a seat")
	}
	if err := c.Attach(pipe(r, "guest")); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	seq, err := c.Submit("move-1", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Sync(); err != nil {
		t.Fatal(err)
	}
	c.Disconnect()
	for i := 0; i < room.PageSize*2+1; i++ {
		if _, e := r.Submit(fmt.Sprint(i), []byte("inc")); e != nil {
			t.Fatal(e)
		}
	}
	if _, s, _ := c.View(); string(s) != "1" {
		t.Fatal("disconnected replica changed")
	}
	if err := c.Attach(pipe(r, "guest")); err != nil {
		t.Fatal(err)
	}
	retry, err := c.Submit("move-1", []byte("inc"))
	if err != nil || retry != seq {
		t.Fatal(retry, err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	a, s, _ := r.View()
	b, v, _ := c.View()
	if a.Version != b.Version || string(s) != "66" || string(v) != string(s) {
		t.Fatalf("host=%d/%s guest=%d/%s", a.Version, s, b.Version, v)
	}
}

func TestGuestCannotStart(t *testing.T) {
	r, _, id := fixture(t)
	s := pipe(r, "guest")
	defer s.Close()
	resp, err := exchange(s, request{Op: "join", Room: id})
	if err != nil || resp.Err != "" {
		t.Fatal(resp, err)
	}
	resp, err = exchange(s, request{Op: "start"})
	if err != nil || resp.Err == "" {
		t.Fatal("remote start not rejected", resp, err)
	}
	if info, _, _ := r.View(); info.Started {
		t.Fatal("remote start committed")
	}
}

func TestCommandCommittedButAckLost(t *testing.T) {
	r, c, id := fixture(t)
	s := pipe(r, "guest")
	res, err := exchange(s, request{Op: "join", Room: id})
	if err != nil || res.Err != "" {
		t.Fatal(res, err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	if err = send(s, request{Op: "submit", ID: "r", Body: []byte("inc")}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	deadline := time.Now().Add(time.Second)
	for {
		a, _, _ := r.View()
		if a.Version == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not committed")
		}
		time.Sleep(time.Millisecond)
	}
	if err = c.Attach(pipe(r, "guest")); err != nil {
		t.Fatal(err)
	}
	if seq, err := c.Submit("r", []byte("inc")); seq != 2 || err != nil {
		t.Fatal(seq, err)
	}
	_, v, _ := r.View()
	if string(v) != "1" {
		t.Fatal("applied twice")
	}
}

func TestInvalidReplayIsAtomic(t *testing.T) {
	_, c, id := fixture(t)
	p := room.Page{Info: room.Info{ID: id, Host: "host", Expires: time.Now().Add(time.Hour), Version: 2}, Events: []room.Event{{Seq: 1, Kind: "start"}, {Seq: 3, Kind: "game", Body: []byte("+1")}}}
	if c.apply(&p) == nil || c.info.Version != 0 || string(c.state) != "0" {
		t.Fatal("partially applied gap")
	}
}

func TestOversizedFrame(t *testing.T) {
	r, _, _ := fixture(t)
	s := pipe(r, "guest")
	defer s.Close()
	s.SetDeadline(time.Now().Add(time.Second))
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrame+1)
	if _, err := s.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := s.Read(b[:]); err == nil {
		t.Fatal("oversized frame kept stream open")
	}
}

func TestDisconnectedReplicaExpires(t *testing.T) {
	r, err := room.Open("", room.Config{Host: "host", Players: 2, TTL: 150 * time.Millisecond}, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	id, _, _ := r.Invite()
	c := NewClient(counter.Game{}, id, "host", "")
	defer c.Close()
	if err := c.Attach(pipe(r, "guest")); err != nil {
		t.Fatal(err)
	}
	c.Disconnect()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.mu.Lock()
		forgot := c.expired && c.state == nil && c.info.ID == ""
		c.mu.Unlock()
		if forgot {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("idle guest did not discard replica")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
