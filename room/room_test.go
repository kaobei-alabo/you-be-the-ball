package room_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

func openRoom(t *testing.T, dir string, c room.Config) *room.Room {
	t.Helper()
	r, err := room.Open(dir, c, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func join(t *testing.T, r *room.Room, p string) *room.Seat {
	t.Helper()
	id, s, err := r.Invite()
	if err != nil {
		t.Fatal(err)
	}
	seat, err := r.Join(p, id, s)
	if err != nil {
		t.Fatal(err)
	}
	return seat
}

func TestLobbyRacesAndSessionReplacement(t *testing.T) {
	r := openRoom(t, "", room.Config{Host: "h", Players: 2})
	if r.Start() == nil {
		t.Fatal("started early")
	}
	if _, err := r.Submit("x", []byte("inc")); err == nil {
		t.Fatal("accepted lobby command")
	}
	id, _, _ := r.Invite()
	var wg sync.WaitGroup
	winners := make(chan string, 2)
	for _, p := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Join(p, id, ""); err == nil {
				winners <- p
			}
		}()
	}
	wg.Wait()
	close(winners)
	if len(winners) != 1 {
		t.Fatalf("%d winners", len(winners))
	}
	p := <-winners
	old := join(t, r, p)
	current := join(t, r, p)
	old.Leave()
	if _, err := old.Since(0); err == nil {
		t.Fatal("stale session accepted")
	}
	current.Leave()
	if r.Start() == nil {
		t.Fatal("started after disconnect")
	}
	join(t, r, p)
	if e := r.Start(); e != nil {
		t.Fatal(e)
	}
	if r.Start() == nil {
		t.Fatal("started twice")
	}
	if _, err := r.Join("new", id, ""); err == nil {
		t.Fatal("new peer took a locked seat")
	}
}

func TestPrivateRoom(t *testing.T) {
	r := openRoom(t, "", room.Config{Host: "h", Players: 2, Private: true})
	id, secret, _ := r.Invite()
	if secret == "" {
		t.Fatal("empty secret")
	}
	if _, err := r.Listing(); err == nil {
		t.Fatal("private room listed")
	}
	for _, bad := range []string{"", "not my invite"} {
		if _, err := r.Join("g", id, bad); err == nil {
			t.Fatal("invalid invite accepted")
		}
	}
	join(t, r, "g")
}

func TestPersistentRestartAndReceipts(t *testing.T) {
	dir := t.TempDir()
	c := room.Config{Host: "h", Players: 2}
	r := openRoom(t, dir, c)
	info, _, _ := r.View()
	g := join(t, r, "g")
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	seq, err := g.Submit("id", []byte("inc"))
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	r = openRoom(t, dir, c)
	g = join(t, r, "g")
	retry, err := g.Submit("id", []byte("inc"))
	if retry != seq || err != nil {
		t.Fatalf("retry=%d err=%v", retry, err)
	}
	if _, err = r.Submit("id", []byte("inc")); err != nil {
		t.Fatal(err)
	}
	cur, state, err := r.View()
	if err != nil || cur.ID != info.ID || !cur.Expires.Equal(info.Expires) || cur.Version != 3 || string(state) != "2" {
		t.Fatalf("restart: %+v %s %v", cur, state, err)
	}
}

func TestExpiryCannotBeExtended(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	c := room.Config{Host: "h", Players: 2, Now: func() time.Time { return now }}
	r := openRoom(t, dir, c)
	id, _, _ := r.Invite()
	now = now.Add(room.MaxTTL - time.Second)
	join(t, r, "g")
	now = now.Add(time.Second)
	if _, _, err := r.View(); !errors.Is(err, room.ErrExpired) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "room.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired data on disk", err)
	}
	r.Close()
	r = openRoom(t, dir, c)
	next, _, _ := r.Invite()
	if next == id {
		t.Fatal("reused expired room")
	}
	if _, err := room.Open("", room.Config{Host: "h", Players: 2, TTL: room.MaxTTL + 1}, counter.Game{}); err == nil {
		t.Fatal("allowed excessive ttl")
	}
}

func TestExpiredWhilePoweredOff(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	c := room.Config{Host: "h", Players: 2, Now: func() time.Time { return now }}
	r := openRoom(t, dir, c)
	id, _, _ := r.Invite()
	r.Close()
	now = now.Add(4 * 24 * time.Hour)
	r = openRoom(t, dir, c)
	next, _, _ := r.Invite()
	if id == next {
		t.Fatal("reopened expired room")
	}
}

func TestCommitFailureIsNotAcknowledged(t *testing.T) {
	dir := t.TempDir()
	r := openRoom(t, dir, room.Config{Host: "h", Players: 2})
	join(t, r, "g")
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(dir, "room.json.pending")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit("x", []byte("inc")); err == nil {
		t.Fatal("ack despite failed write")
	}
	info, state, _ := r.View()
	if info.Version != 1 || string(state) != "0" {
		t.Fatal(info, state)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if seq, err := r.Submit("x", []byte("inc")); err != nil || seq != 2 {
		t.Fatal(seq, err)
	}
}

func TestIdleExpiryRemovesHostFile(t *testing.T) {
	dir := t.TempDir()
	openRoom(t, dir, room.Config{Host: "host", Players: 2, TTL: 80 * time.Millisecond})
	path := filepath.Join(dir, "room.json")
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("idle host did not purge file")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
