package lan

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/kaobei-alabo/you-be-the-ball/game"
	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// delayNetwork makes every node created until the test ends send each byte of
// every stream d late, in both directions, as a network with a round trip of
// 2d would. The bandwidth stays as it was.
func delayNetwork(t *testing.T, d time.Duration) {
	old := wrapHost
	wrapHost = func(h host.Host) host.Host { return slowHost{h, d} }
	t.Cleanup(func() { wrapHost = old })
}

type slowHost struct {
	host.Host
	d time.Duration
}

func (h slowHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	s, err := h.Host.NewStream(ctx, p, pids...)
	if err != nil {
		return nil, err
	}
	return delayLine(s, h.d), nil
}

func (h slowHost) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	h.Host.SetStreamHandler(pid, func(s network.Stream) { handler(delayLine(s, h.d)) })
}

// delayed is a delay line: what is written reaches the stream d later, in
// order, from a forwarding goroutine, so writes never wait for one another.
// Reads are not delayed here: the other side's writes are.
type delayed struct {
	network.Stream
	d    time.Duration
	mu   sync.Mutex
	q    []chunk
	err  error
	wake chan struct{}
}

type chunk struct {
	at   time.Time
	b    []byte
	stop func() error // Close or CloseWrite, in its place after the data
}

func delayLine(s network.Stream, d time.Duration) network.Stream {
	l := &delayed{Stream: s, d: d, wake: make(chan struct{}, 1)}
	go l.forward()
	return l
}

func (l *delayed) push(c chunk) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	c.at = time.Now().Add(l.d)
	l.q = append(l.q, c)
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}

func (l *delayed) Write(b []byte) (int, error) {
	if err := l.push(chunk{b: slices.Clone(b)}); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (l *delayed) Close() error      { return l.push(chunk{stop: l.Stream.Close}) }
func (l *delayed) CloseWrite() error { return l.push(chunk{stop: l.Stream.CloseWrite}) }

func (l *delayed) Reset() error {
	l.mu.Lock()
	l.err, l.q = network.ErrReset, nil
	l.mu.Unlock()
	return l.Stream.Reset()
}

func (l *delayed) forward() {
	for range l.wake {
		for {
			l.mu.Lock()
			if len(l.q) == 0 || l.err != nil {
				l.mu.Unlock()
				break
			}
			c := l.q[0]
			l.q = l.q[1:]
			l.mu.Unlock()
			time.Sleep(time.Until(c.at))
			var err error
			if c.stop != nil {
				err = c.stop()
			} else {
				_, err = l.Stream.Write(c.b)
			}
			if c.stop != nil || err != nil {
				if err != nil {
					_ = l.Reset()
				}
				return
			}
		}
	}
}

// stamped is the counter, noting when each count first appeared on this copy
// (the last time it was computed: the host also computes it while deciding).
type stamped struct {
	counter.Game
	mu sync.Mutex
	at map[int]time.Time
}

func (g *stamped) Apply(state, event []byte) ([]byte, error) {
	next, err := g.Game.Apply(state, event)
	if err == nil {
		n, _ := strconv.Atoi(string(next))
		g.mu.Lock()
		g.at[n] = time.Now()
		g.mu.Unlock()
	}
	return next, err
}

func quantile(ds []time.Duration, q float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	slices.Sort(ds)
	return ds[int(q*float64(len(ds)-1))]
}

// TestEveryoneMovesAtOnceUnderDelay measures a room where every player moves
// at the same time, over a simulated network. It runs only with MEASURE=1;
// MEASURE_PLAYERS and MEASURE_DELAYS (comma-separated) narrow it.
func TestEveryoneMovesAtOnceUnderDelay(t *testing.T) {
	if os.Getenv("MEASURE") == "" {
		t.Skip("set MEASURE=1")
	}
	players := []int{2, 4, 8, 16, 32}
	delays := []time.Duration{0, time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond}
	if s := os.Getenv("MEASURE_PLAYERS"); s != "" {
		players = nil
		for _, f := range strings.Split(s, ",") {
			n, _ := strconv.Atoi(f)
			players = append(players, n)
		}
	}
	if s := os.Getenv("MEASURE_DELAYS"); s != "" {
		delays = nil
		for _, f := range strings.Split(s, ",") {
			d, _ := time.ParseDuration(f)
			delays = append(delays, d)
		}
	}
	t.Log("players  one-way  moves/s  host p50/p99  guest p50/p99  errors  visible p50/p99")
	for _, n := range players {
		for _, d := range delays {
			t.Run(fmt.Sprint(n, "x", d), func(t *testing.T) { measure(t, n, d) })
		}
	}
}

func measure(t *testing.T, n int, d time.Duration) {
	if d > 0 {
		delayNetwork(t, d)
	}
	const workers, span = 4, 3 * time.Second
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = newNode(t, t.TempDir())
	}
	games := make([]*stamped, n)
	ps := playGames(t, nodes, room.Config{Players: n}, Timing{}, func(i int) game.Game {
		games[i] = &stamped{at: make(map[int]time.Time)}
		return games[i]
	})
	var mu sync.Mutex
	var host, guest []time.Duration
	var acked, errs atomic.Int64
	begin := time.Now()
	end := begin.Add(span)
	var wg sync.WaitGroup
	for i, p := range ps {
		for w := range workers {
			wg.Go(func() {
				for k := 0; time.Now().Before(end); k++ {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					t0 := time.Now()
					_, err := p.Submit(ctx, fmt.Sprint(w, "-", k), []byte("inc"))
					took := time.Since(t0)
					cancel()
					if err != nil {
						errs.Add(1)
						continue
					}
					if time.Now().Before(end) {
						acked.Add(1)
					}
					mu.Lock()
					if i == 0 {
						host = append(host, took)
					} else {
						guest = append(guest, took)
					}
					mu.Unlock()
				}
			})
		}
	}
	wg.Wait()
	if !ps[0].Status().IsHost {
		t.Error("the host changed during the measurement")
	}
	total := len(host) + len(guest)
	if errs.Load() == 0 {
		converged(t, ps, strconv.Itoa(total))
	}
	vis := "-"
	if n == 8 {
		var lags []time.Duration
		h := games[0]
		for _, g := range games[1:] {
			for k := 1; k <= total; k++ {
				if a, ok := g.at[k]; ok {
					lags = append(lags, a.Sub(h.at[k]))
				}
			}
		}
		vis = fmt.Sprintf("%v/%v", quantile(lags, .5).Round(10*time.Microsecond), quantile(lags, .99).Round(10*time.Microsecond))
	}
	ps[0].mu.Lock()
	rounds, proposed := ps[0].rounds, ps[0].proposed
	ps[0].mu.Unlock()
	t.Logf("%d rounds, %.1f moves per round", rounds, float64(proposed)/float64(max(rounds, 1)))
	r := func(x time.Duration) time.Duration { return x.Round(10 * time.Microsecond) }
	t.Logf("%7d  %7v  %7.0f  %v/%v  %v/%v  %d  %s", n, d, float64(acked.Load())/span.Seconds(),
		r(quantile(host, .5)), r(quantile(host, .99)), r(quantile(guest, .5)), r(quantile(guest, .99)), errs.Load(), vis)
}
