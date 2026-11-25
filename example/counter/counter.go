// Package counter is an example game, not part of the room protocol.
package counter

import (
	"errors"
	"strconv"
)

type Game struct{}

func (Game) Initial() []byte { return []byte("0") }
func (Game) Decide(_ []byte, _ string, cmd []byte) ([]byte, error) {
	if string(cmd) != "inc" {
		return nil, errors.New("expected inc")
	}
	return []byte("+1"), nil
}

func (Game) Apply(state, event []byte) ([]byte, error) {
	n, err := strconv.Atoi(string(state))
	if err != nil || string(event) != "+1" {
		return nil, errors.New("invalid counter event")
	}
	return []byte(strconv.Itoa(n + 1)), nil
}
