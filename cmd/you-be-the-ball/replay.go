package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/internal/home"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// replay lists the kept games or plays one back. Without -profile or -dir it
// looks in every profile: a game may have been played as host or as a guest.
func replay(root string, args []string, stdout, stderr io.Writer) error {
	cfg, err := home.LoadConfig(root)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("you-be-the-ball replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	profile := fs.String("profile", "", "profile name under ~/.you-be-the-ball (default: every profile)")
	dir := fs.String("dir", "", "explicit data directory instead of a profile")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: you-be-the-ball replay [-profile NAME | -dir PATH] [ROOM_ID | FILE.moves]")
	}
	named := false
	fs.Visit(func(f *flag.Flag) { named = named || f.Name == "profile" || f.Name == "dir" })
	dirs, where := []string{root}, root
	if named {
		d, err := dataDir(fs, root, *profile, *dir)
		if err != nil {
			return err
		}
		dirs, where = []string{d}, d
	} else if dirs, err = profiles(root); err != nil {
		return err
	}
	for _, d := range dirs {
		_, _ = room.PurgeMoves(d, time.Duration(cfg.KeepMoves), time.Now())
	}
	if fs.NArg() == 0 {
		return list(dirs, where, !named, stdout)
	}
	file := fs.Arg(0)
	if !strings.HasSuffix(file, ".moves") {
		if file, err = locate(dirs, file); err != nil {
			return err
		}
	}
	return playBack(file, stdout)
}

// profiles are the folders under root.
func profiles(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(root, e.Name()))
		}
	}
	return dirs, err
}

type kept struct {
	id, dir, status string
	at              time.Time
}

// list prints the games with a moves file, newest first.
func list(dirs []string, where string, named bool, stdout io.Writer) error {
	var games []kept
	for _, d := range dirs {
		files, _ := filepath.Glob(filepath.Join(d, "games", "*.moves"))
		for _, f := range files {
			fi, err := os.Stat(f)
			if err != nil {
				continue
			}
			k := kept{id: strings.TrimSuffix(filepath.Base(f), ".moves"), dir: d, status: "no result", at: fi.ModTime()}
			if r, err := room.ReadRecord(strings.TrimSuffix(f, ".moves") + ".json"); err == nil {
				k.status = r.Status
			}
			games = append(games, k)
		}
	}
	if len(games) == 0 {
		fmt.Fprintf(stdout, "No replays in %s.\n", where)
		return nil
	}
	slices.SortFunc(games, func(a, b kept) int { return b.at.Compare(a.at) })
	for _, k := range games {
		line := fmt.Sprintf("%s  %s  %s", k.id, k.at.Format(time.RFC3339), k.status)
		if named {
			line += "  (" + filepath.Base(k.dir) + ")"
		}
		fmt.Fprintln(stdout, line)
	}
	return nil
}

// locate is the moves file of a room in one of dirs.
func locate(dirs []string, id string) (string, error) {
	var found []string
	for _, d := range dirs {
		f := filepath.Join(d, "games", id+".moves")
		if _, err := os.Stat(f); err == nil {
			found = append(found, f)
		}
	}
	switch len(found) {
	case 0:
		return "", fmt.Errorf("no replay of room %s; \"replay\" lists them", id)
	case 1:
		return found[0], nil
	}
	var names []string
	for _, f := range found {
		names = append(names, filepath.Base(filepath.Dir(filepath.Dir(f))))
	}
	return "", fmt.Errorf("room %s is in profiles %s: choose one with -profile", id, strings.Join(names, ", "))
}

var errHeader = errors.New("header only")

func playBack(file string, stdout io.Writer) error {
	// The first pass reads only the header, so it is printed before the moves.
	info, err := room.Replay(play.Game, file, func(room.Move, []byte) error { return errHeader })
	if err != nil && err != errHeader {
		return err
	}
	players := make([]string, len(info.Players))
	for i, p := range info.Players {
		players[i] = short(p, "")
	}
	fmt.Fprintf(stdout, "Room %s, players %s\n", info.ID, strings.Join(players, " "))
	n := 0 // game moves: not the finish or expiry
	_, err = room.Replay(play.Game, file, func(m room.Move, state []byte) error {
		if m.Kind == "" {
			n++
		}
		fmt.Fprintf(stdout, "v%d %s\n", m.Seq, cmp.Or(m.Kind, short(m.Peer, "")+" "+m.ID+" "+view(state)))
		return nil
	})
	if err == nil {
		fmt.Fprintln(stdout, n, "moves replayed")
	}
	return err
}
