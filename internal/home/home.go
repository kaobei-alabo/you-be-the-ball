// Package home locates the per-user CLI state. Never infer a peer identity from the working directory.
package home

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const basename = ".you-be-the-ball"

func Root() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate user home: %w", err)
	}
	if !filepath.IsAbs(home) {
		return "", fmt.Errorf("user home must be absolute, got %q", home)
	}
	return filepath.Join(home, basename), nil
}

// ProfileDir restricts names to a single component, including on Windows.
func ProfileDir(root, name string) (string, error) {
	if !filepath.IsLocal(name) || len(name) > 64 || strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" {
		return "", fmt.Errorf("invalid profile %q: use 1–64 letters, digits, hyphens or underscores", name)
	}
	return filepath.Join(root, name), nil
}

type Config struct {
	HostListen string `json:"host_listen"`
	PeerListen string `json:"peer_listen"`
	Players    int    `json:"players"`
	MDNS       bool   `json:"mdns"`
	Private    bool   `json:"private"`
}

// LoadConfig overlays an optional config.json on the defaults. Merely looking up
// the path (including requesting CLI help) never creates files or a peer identity.
func LoadConfig(root string) (Config, error) {
	c := Config{HostListen: "/ip4/0.0.0.0/tcp/42042", PeerListen: "/ip4/0.0.0.0/tcp/0", Players: 2, MDNS: true}
	path := filepath.Join(root, "config.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return Config{}, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	// Decode into a pointer so a top-level null is not silently treated as defaults.
	out := &c
	if err = d.Decode(&out); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	if out == nil {
		return Config{}, fmt.Errorf("%s: expected a JSON object", path)
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return Config{}, fmt.Errorf("%s: expected exactly one JSON object", path)
	}
	if c.Players < 2 || c.Players > 32 || c.HostListen == "" || c.PeerListen == "" {
		return Config{}, fmt.Errorf("%s: players must be 2..32 and listen addresses must not be empty", path)
	}
	return c, nil
}

// Migrate moves an explicitly named legacy profile intact. No scanning or copying
// of private keys, no overwrite, and no copy/delete fallback across filesystems.
// The caller must stop processes using the source before running this operation.
func Migrate(root, source string) (string, error) {
	source, err := filepath.Abs(source)
	if err != nil {
		return "", err
	}
	name, ok := strings.CutPrefix(filepath.Base(source), ".p2p-")
	if !ok {
		return "", fmt.Errorf("not a legacy .p2p-* directory: %s", source)
	}
	dest, err := ProfileDir(root, name)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(source)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %s", source)
	}
	key := filepath.Join(source, "identity.key")
	info, err = os.Lstat(key)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular identity file: %s", key)
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return "", err
	}
	if _, err = os.Lstat(dest); err == nil {
		return "", fmt.Errorf("destination already exists; not moving %s to %s", source, dest)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err = os.Rename(source, dest); err != nil {
		return "", fmt.Errorf("move profile: %w", err)
	}
	return dest, nil
}
