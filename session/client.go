package session

import (
	"bytes"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// Client retains its replica across stream replacements; a new client can replay from zero.
type Client struct {
	mu               sync.Mutex
	game             game.Game
	id, host, secret string
	conn             Stream
	info             room.Info
	state            []byte
	timer            *time.Timer
	expired          bool
}

func NewClient(g game.Game, id, host, secret string) *Client {
	return &Client{game: g, id: id, host: host, secret: secret, state: bytes.Clone(g.Initial())}
}

func (c *Client) drop() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) forget() {
	c.drop()
	c.expired = true
	c.info = room.Info{}
	c.state = nil
	c.secret = ""
}

func (c *Client) check() error {
	if c.expired || (!c.info.Expires.IsZero() && !time.Now().Before(c.info.Expires)) {
		c.forget()
		return room.ErrExpired
	}
	return nil
}

func (c *Client) Disconnect() { c.mu.Lock(); defer c.mu.Unlock(); c.drop() }
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.timer != nil {
		c.timer.Stop()
	}
	c.forget()
}

// Attach takes ownership even on error. The transport must authenticate the expected host.
func (c *Client) Attach(s Stream) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drop()
	c.conn = s
	if err := c.check(); err != nil {
		return err
	}
	return c.catchUp(request{Op: "join", Room: c.id, Secret: c.secret, After: c.info.Version})
}

func (c *Client) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(); err != nil {
		return err
	}
	return c.catchUp(request{Op: "sync", After: c.info.Version})
}

func (c *Client) catchUp(q request) error {
	if c.conn == nil {
		return errors.New("disconnected")
	}
	for {
		res, err := exchange(c.conn, q)
		if err == nil && res.Err != "" {
			err = errors.New(res.Err)
		}
		if err == nil {
			err = c.apply(res.Page)
		}
		if err != nil {
			c.drop()
			return err
		}
		if c.info.Version == res.Page.Info.Version {
			return nil
		}
		q = request{Op: "sync", After: c.info.Version}
	}
}

func (c *Client) apply(p *room.Page) error {
	if p == nil {
		return errors.New("missing replay page")
	}
	info := p.Info
	if info.ID != c.id || info.Host != c.host || (!c.info.Expires.IsZero() && !info.Expires.Equal(c.info.Expires)) {
		return errors.New("wrong room, host, or expiry")
	}
	if !time.Now().Before(info.Expires) {
		c.forget()
		return room.ErrExpired
	}
	if info.Expires.After(time.Now().Add(room.MaxTTL + time.Minute)) {
		return errors.New("invalid expiry")
	}
	seq := c.info.Version
	if len(p.Events) > room.PageSize || info.Version < seq || len(p.Events) != min(info.Version-seq, room.PageSize) {
		return errors.New("incomplete replay page")
	}
	state := bytes.Clone(c.state)
	for _, e := range p.Events {
		if e.Seq != seq+1 {
			return errors.New("event out of order")
		}
		if e.Seq == 1 {
			if e.Kind != "start" {
				return errors.New("missing start event")
			}
		} else {
			if e.Kind != "game" {
				return errors.New("unexpected event")
			}
			var err error
			state, err = c.game.Apply(state, e.Body)
			if err != nil {
				return err
			}
		}
		seq++
	}
	if c.timer == nil {
		c.timer = time.AfterFunc(time.Until(info.Expires), func() { c.mu.Lock(); defer c.mu.Unlock(); c.forget() })
	}
	info.Version = seq
	c.state = state
	c.info = info
	return nil
}

func (c *Client) Submit(id string, b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(); err != nil {
		return 0, err
	}
	if c.conn == nil {
		return 0, errors.New("disconnected")
	}
	res, err := exchange(c.conn, request{Op: "submit", ID: id, Body: b})
	if err != nil {
		c.drop()
		return 0, err
	}
	if res.Err != "" {
		return 0, errors.New(res.Err)
	}
	if res.Seq <= 0 {
		return 0, errors.New("missing receipt")
	}
	return res.Seq, nil
}

func (c *Client) View() (room.Info, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(); err != nil {
		return room.Info{}, nil, err
	}
	if c.info.ID == "" {
		return room.Info{}, nil, errors.New("not joined")
	}
	info := c.info
	info.Roster = slices.Clone(info.Roster)
	info.Online = slices.Clone(info.Online)
	return info, bytes.Clone(c.state), nil
}
