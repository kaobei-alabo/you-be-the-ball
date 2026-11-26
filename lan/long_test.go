package lan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// padded counts moves of any payload: the event is the command itself.
type padded struct{}

func (padded) Initial() []byte { return []byte("0") }
func (padded) Decide(_ []byte, _ string, cmd []byte) ([]byte, error) {
	return cmd, nil
}
func (padded) Apply(state, _ []byte) ([]byte, error) {
	n, err := strconv.Atoi(string(state))
	if err != nil {
		return nil, errors.New("invalid state")
	}
	return []byte(strconv.Itoa(n + 1)), nil
}

func heap() string {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("%.1f MiB", float64(m.HeapInuse)/(1<<20))
}

// TestLongGame plays long games with the production compaction settings. It
// takes about 35 s, so it runs only with LONG_GAME=1.
func TestLongGame(t *testing.T) {
	if os.Getenv("LONG_GAME") == "" {
		t.Skip("set LONG_GAME=1")
	}
	for _, c := range []struct {
		moves, size int
		rejoin      bool
	}{{200_000, 8, false}, {20_000, room.MaxPayload, false}, {200_000, 8, true}, {20_000, room.MaxPayload, true}} {
		t.Run(fmt.Sprint(c.moves, "x", c.size, " rejoin=", c.rejoin), func(t *testing.T) { long(t, c.moves, c.size, c.rejoin) })
	}
}

func long(t *testing.T, total, size int, rejoin bool) {
	old := theGame
	theGame = padded{}
	t.Cleanup(func() { theGame = old })
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	nodes := make([]*Node, 3)
	for i := range nodes {
		nodes[i] = newNode(t, dirs[i])
	}
	ps := playOn(t, nodes, room.Config{Players: 3}, Timing{})
	inv := original(t, ps[0])
	start := heap()
	payload := bytes.Repeat([]byte("x"), size)
	var next atomic.Int64
	var mu sync.Mutex // guards ps while a player is replaced
	run := func(upto int64) {
		var wg sync.WaitGroup
		for w := range 12 {
			wg.Go(func() {
				for {
					n := next.Add(1)
					if n > upto {
						next.Add(-1)
						return
					}
					mu.Lock()
					p := ps[w%3]
					mu.Unlock()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					_, err := p.Submit(ctx, fmt.Sprint("w", w, "-", n), payload)
					cancel()
					if err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
	}
	begin := time.Now()
	run(int64(total / 2))
	middle := heap()
	if rejoin {
		crash(ps[2])
		ps[2].logs.mu.Lock()
		ps[2].logs.logs = nil // gone with the crashed program, not kept by the test
		ps[2].logs.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		c, err := Join(ctx, newNode(t, dirs[2]), theGame, inv, Options{Dir: t.TempDir()})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		mu.Lock()
		ps[2] = c
		mu.Unlock()
		lo, _, _ := span(c)
		t.Logf("rejoined at v%d with log from %d", c.Status().Version, lo)
	}
	run(int64(total))
	d := time.Since(begin)
	converged(t, ps, strconv.Itoa(total))
	if err := leader(t, ps...).Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	records(t, ps, strconv.Itoa(total))
	for i, p := range ps {
		p.logs.mu.Lock()
		t.Logf("player %d: peak %d entries, %d after the end", i, p.logs.peak, len(p.logs.logs))
		p.logs.mu.Unlock()
	}
	t.Logf("%d moves of %d B in %v: %.0f moves/s; heap in use: start %s, middle %s, end %s",
		total, size, d.Round(time.Millisecond), float64(total)/d.Seconds(), start, middle, heap())
}
