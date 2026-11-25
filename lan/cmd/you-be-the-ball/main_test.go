package main

import (
	"os"
	"path/filepath"
	"testing"

	"you-be-the-ball.local/core/example/counter"
	"you-be-the-ball.local/core/room"
	"you-be-the-ball.local/lan/internal/apphome"
)

func fakeHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	root, err := apphome.Root()
	if err != nil {
		t.Fatal(err)
	}
	if root != filepath.Join(home, ".you-be-the-ball") {
		t.Fatal(root)
	}
	return root
}

func TestHomeProfilesIgnoreWorkingDirectory(t *testing.T) {
	root := fakeHome(t)
	for range 2 {
		cwd := t.TempDir()
		t.Chdir(cwd)
		for _, mode := range []string{"host", "join", "find"} {
			var args []string
			if mode == "join" {
				args = []string{"invite"}
			}
			o, err := parseOptions(root, mode, args)
			if err != nil || o.dir != filepath.Join(root, mode) {
				t.Fatalf("%s: %+v, %v", mode, o, err)
			}
		}
		entries, err := os.ReadDir(cwd)
		if err != nil || len(entries) != 0 {
			t.Fatal("unexpected working directory state", entries, err)
		}
	}
	custom, err := parseOptions(root, "host", []string{"-profile", "colleague-test"})
	if err != nil || custom.dir != filepath.Join(root, "colleague-test") {
		t.Fatal(custom, err)
	}
	for _, args := range [][]string{{"-profile", "../not-a-profile"}, {"-profile", "."}, {"-profile", ""}, {"-profile", "alice", "-dir", "."}, {"-dir", ""}} {
		if _, err := parseOptions(root, "host", args); err == nil {
			t.Fatal("accepted", args)
		}
	}
	explicit := t.TempDir()
	custom, err = parseOptions(root, "host", []string{"-dir", explicit})
	if err != nil || custom.dir != explicit {
		t.Fatal(custom, err)
	}
}

func TestHomeConfigAndFlagPrecedence(t *testing.T) {
	root := fakeHome(t)
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"players":4,"private":true,"mdns":false,"host_listen":"/ip4/127.0.0.1/tcp/42345"}`), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := parseOptions(root, "host", nil)
	if err != nil || !h.private || h.mdns || h.players != 4 || h.listen != "/ip4/127.0.0.1/tcp/42345" {
		t.Fatal(h, err)
	}
	h, err = parseOptions(root, "host", []string{"-players", "3", "-private=false", "-mdns"})
	if err != nil || h.private || !h.mdns || h.players != 3 {
		t.Fatal(h, err)
	}
	peer, err := parseOptions(root, "join", []string{"invite"})
	if err != nil || peer.listen != "/ip4/0.0.0.0/tcp/0" {
		t.Fatal(peer, err)
	}
	for _, invalid := range []string{`{`, `{"typo":true}`, `{} {}`, `null`, `{"players":1}`} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseOptions(root, "host", nil); err == nil {
			t.Fatalf("accepted invalid config: %s", invalid)
		}
	}
}

func TestMigrateResumesRoomWithoutOverwriting(t *testing.T) {
	root := fakeHome(t)
	old := filepath.Join(t.TempDir(), ".p2p-host")
	cfg := room.Config{Host: "host", Players: 2}
	r, err := room.Open(old, cfg, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	id, sec, err := r.Invite()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Join("guest", id, sec); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit("before-rename", []byte("inc")); err != nil {
		t.Fatal(err)
	}
	before, _, err := r.View()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err := os.WriteFile(filepath.Join(old, "identity.key"), []byte("fixture-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"migrate", old}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old dir still present", err)
	}
	o, err := parseOptions(root, "host", nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(filepath.Join(o.dir, "identity.key"))
	if err != nil || string(key) != "fixture-key" {
		t.Fatal("identity was not preserved", err)
	}
	r, err = room.Open(o.dir, cfg, counter.Game{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	after, state, err := r.View()
	if err != nil || after.ID != before.ID || !after.Expires.Equal(before.Expires) || after.Version != before.Version || string(state) != "1" {
		t.Fatal(after, string(state), err)
	}
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "identity.key"), []byte("different-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"migrate", old}); err == nil {
		t.Fatal("allowed overwrite")
	}
	key, err = os.ReadFile(filepath.Join(o.dir, "identity.key"))
	if err != nil || string(key) != "fixture-key" {
		t.Fatal("destination key changed")
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatal("source lost", err)
	}
}
