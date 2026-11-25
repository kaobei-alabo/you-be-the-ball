package room

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func remove(file string) error {
	if file == "" {
		return nil
	}
	err := os.Remove(file)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func load(dir string) (*record, string, error) {
	if dir == "" {
		return nil, "", nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, "", err
	}
	file := filepath.Join(dir, "room.json")
	if err := remove(file + ".pending"); err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		return nil, file, nil
	}
	if err != nil {
		return nil, "", err
	}
	var r record
	if err = json.Unmarshal(data, &r); err != nil {
		return nil, "", fmt.Errorf("read room: %w", err)
	}
	return &r, file, nil
}

func save(file string, r record) error {
	if file == "" {
		return nil
	}
	tmp := file + ".pending"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	err = json.NewEncoder(f).Encode(r)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, file)
}
