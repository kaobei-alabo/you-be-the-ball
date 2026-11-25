package room

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Record is what a member keeps on disk once a game is over. Nothing is written
// while the game is being played.
type Record struct {
	ID               string
	Status           string // "finished", "expired", or "interrupted" if this member left before the end
	Players          []string
	Created, Expires time.Time
	Version          int
	State            []byte
	Events           []Event
}

// Record describes the game as this member last saw it.
func (s *State) Record() Record {
	status := "interrupted"
	switch s.phase {
	case Finished:
		status = "finished"
	case Expired:
		status = "expired"
	}
	info := s.Info()
	return Record{ID: info.ID, Status: status, Players: info.Roster, Created: s.setup.Created, Expires: info.Expires, Version: info.Version, State: s.View(), Events: s.Events(0)}
}

// WriteRecord saves r as dir/games/<ID>.json, replacing any earlier record of the
// same game. The file is complete or absent, never partial.
func WriteRecord(dir string, r Record) (string, error) {
	if !validID(r.ID) {
		return "", errors.New("invalid record ID")
	}
	dir = filepath.Join(dir, "games")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	file := filepath.Join(dir, r.ID+".json")
	tmp, err := os.CreateTemp(dir, ".pending-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	err = enc.Encode(r)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	return file, os.Rename(tmp.Name(), file)
}

// ReadRecord loads a record written by WriteRecord.
func ReadRecord(file string) (Record, error) {
	var r Record
	data, err := os.ReadFile(file)
	if err == nil {
		err = json.Unmarshal(data, &r)
	}
	return r, err
}
