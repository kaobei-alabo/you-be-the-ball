package lan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kaobei-alabo/you-be-the-ball/game/counter"
	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// burst is one player moving from several goroutines at once, each move
// retried with its ID until it is acknowledged.
type burst struct {
	wg   sync.WaitGroup
	stop chan struct{}
	mu   sync.Mutex
	seqs []int // acknowledged
	over int   // moves refused because the game is over
}

// moving starts k goroutines on p making n moves each, back to back, until stop.
func moving(t *testing.T, p *Player, k, n int) *burst {
	b := &burst{stop: make(chan struct{})}
	for w := range k {
		b.wg.Go(func() {
			for i := range n {
				select {
				case <-b.stop:
					return
				default:
				}
				id := fmt.Sprint("w", w, "-", i)
				for end := time.Now().Add(30 * time.Second); ; {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					seq, err := p.Submit(ctx, id, []byte("inc"))
					cancel()
					b.mu.Lock()
					switch {
					case err == nil:
						b.seqs = append(b.seqs, seq)
					case errors.Is(err, room.ErrOver):
						b.over++
					}
					b.mu.Unlock()
					if errors.Is(err, room.ErrOver) {
						return
					}
					if err == nil {
						break
					}
					if time.Now().After(end) {
						t.Errorf("%s never acknowledged: %v", id, err)
						return
					}
				}
			}
		})
	}
	return b
}

// halt stops the burst and waits for its moves to be acknowledged or refused.
func (b *burst) halt() []int {
	close(b.stop)
	b.wg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seqs
}

func everyone(t *testing.T, ps []*Player, k, n int) []*burst {
	bs := make([]*burst, len(ps))
	for i, p := range ps {
		bs[i] = moving(t, p, k, n)
	}
	return bs
}

func acknowledged(bs []*burst) []int {
	var seqs []int
	for _, b := range bs {
		b.wg.Wait()
		seqs = append(seqs, b.seqs...)
	}
	return seqs
}

// countedOnce checks that the acknowledged moves are exactly the moves the
// game holds, each at its own step, on every copy.
func countedOnce(t *testing.T, ps []*Player, seqs []int) {
	t.Helper()
	converged(t, ps, strconv.Itoa(len(seqs)))
	if v := ps[0].Status().Version; v != len(seqs)+1 {
		t.Fatalf("version %d after %d moves", v, len(seqs))
	}
	slices.Sort(seqs)
	for i, s := range seqs {
		if s != i+2 {
			t.Fatalf("acknowledged steps are not 2..%d: %d at %d", len(seqs)+1, s, i)
		}
	}
}

func TestEveryoneMovesAtOnce(t *testing.T) {
	ps := play(t, room.Config{Players: 8}, fast)
	countedOnce(t, ps, acknowledged(everyone(t, ps, 4, 100)))
}

func TestThirtyTwoPlayersMoveAtOnce(t *testing.T) {
	ps := play(t, room.Config{Players: 32}, fast)
	countedOnce(t, ps, acknowledged(everyone(t, ps, 2, 10)))
}

func roundsOf(p *Player) (rounds, moves int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rounds, p.proposed
}

func TestMovesAreAgreedInRounds(t *testing.T) {
	delayNetwork(t, 5*time.Millisecond)
	ps := play(t, room.Config{Players: 4}, fast)
	h := ps[0]
	countedOnce(t, ps, acknowledged(everyone(t, ps, 4, 10)))
	rounds, moves := roundsOf(h)
	t.Logf("%d moves in %d rounds", moves, rounds)
	// Every entry counted: none was decided against a state it did not reach.
	// Nothing here is Stale or busy: one host, and every move ID is new.
	if moves != 160 || moves < 3*rounds {
		t.Fatalf("%d moves in %d rounds: moves made at once were not agreed together", moves, rounds)
	}
	for i := range 10 {
		inc(t, ps[1+i%3], fmt.Sprint("alone", i))
	}
	r, m := roundsOf(h)
	if r-rounds != 10 || m-moves != 10 {
		t.Fatalf("10 moves one at a time took %d rounds of %d moves", r-rounds, m-moves)
	}
}

func TestHostChangeWhileEveryoneMoves(t *testing.T) {
	ps := play(t, room.Config{Players: 5}, fast)
	bs := everyone(t, ps, 3, 1_000_000)
	total := func() int {
		n := 0
		for _, b := range bs {
			b.mu.Lock()
			n += len(b.seqs)
			b.mu.Unlock()
		}
		return n
	}
	more := func() {
		n := total() + 200
		waitFor(t, "more moves", func() bool { return total() >= n })
	}
	more()
	// A crashed program acknowledges nothing: its own moves stop first.
	old := leader(t, ps...)
	i := slices.Index(ps, old)
	seqs := bs[i].halt()
	crash(old)
	live := without(ps, old)
	more()
	cutOff := leader(t, live...)
	cutOff.n.SetOffline(true)
	leader(t, without(live, cutOff)...)
	more()
	cutOff.n.SetOffline(false)
	more()
	for j, b := range bs {
		if j != i {
			seqs = append(seqs, b.halt()...)
		}
	}
	countedOnce(t, live, seqs)
	t.Logf("%d moves acknowledged", len(seqs))
}

// claims is a game where each thing goes to the first player who claims it.
type claims struct{}

var errClaimed = errors.New("already claimed")

func (claims) Initial() []byte { return []byte("{}") }
func (claims) Decide(state []byte, actor string, cmd []byte) ([]byte, error) {
	var owner map[string]string
	if err := json.Unmarshal(state, &owner); err != nil {
		return nil, err
	}
	if _, ok := owner[string(cmd)]; ok {
		return nil, errClaimed
	}
	return json.Marshal([2]string{string(cmd), actor})
}
func (claims) Apply(state, event []byte) ([]byte, error) {
	var owner map[string]string
	var claim [2]string
	if err := json.Unmarshal(state, &owner); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(event, &claim); err != nil {
		return nil, err
	}
	if _, ok := owner[claim[0]]; ok {
		return nil, errClaimed
	}
	owner[claim[0]] = claim[1]
	return json.Marshal(owner)
}

// Two players claim the same thing at the same time: the game's rules decide,
// in the order the host received the claims. The first wins; the others are
// told so by the game, as if they had claimed it later.
func TestSimultaneousClaimHasOneWinner(t *testing.T) {
	old := theGame
	theGame = claims{}
	t.Cleanup(func() { theGame = old })
	ps := play(t, room.Config{Players: 4}, fast)
	want := map[string]string{}
	for x := range 30 {
		thing := fmt.Sprint("thing", x)
		now := make(chan struct{})
		errs := make([]error, len(ps))
		var wg sync.WaitGroup
		for i, p := range ps {
			wg.Go(func() {
				<-now
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_, errs[i] = p.Submit(ctx, thing, []byte(thing))
			})
		}
		close(now)
		wg.Wait()
		winners := 0
		for i, err := range errs {
			switch {
			case err == nil:
				winners++
				want[thing] = id(ps[i])
			case err.Error() != errClaimed.Error():
				t.Fatalf("%s: player %d got %v", thing, i, err)
			}
		}
		if winners != 1 {
			t.Fatalf("%s: %d winners", thing, winners)
		}
	}
	b, _ := json.Marshal(want)
	converged(t, ps, string(b))
}

func TestSameMoveSentTwiceAtOnce(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	for x := range 20 {
		p := ps[x%2] // the host and a guest
		now := make(chan struct{})
		seqs := make([]int, 4)
		var wg sync.WaitGroup
		for i := range seqs {
			wg.Go(func() {
				<-now
				seqs[i] = inc(t, p, fmt.Sprint("twice", x))
			})
		}
		close(now)
		wg.Wait()
		for _, s := range seqs {
			if s != x+2 {
				t.Fatalf("move %d: sequence numbers %v", x, seqs)
			}
		}
	}
	converged(t, ps, "20")
}

func TestFinishWhileEveryoneMoves(t *testing.T) {
	ps := play(t, room.Config{Players: 4}, fast)
	bs := everyone(t, ps, 3, 1_000_000)
	waitFor(t, "some moves", func() bool { return ps[0].Status().Version > 300 })
	if err := ps[0].Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	seqs, over := acknowledged(bs), 0
	for _, b := range bs {
		over += b.over
	}
	if over != len(bs)*3 {
		t.Fatalf("%d of %d movers were told the game is over", over, len(bs)*3)
	}
	result := strconv.Itoa(len(seqs))
	records(t, ps, result)
	for _, p := range ps {
		if v := p.Status().Version; v != len(seqs)+2 {
			t.Fatalf("version %d after %d moves and the finish", v, len(seqs))
		}
		moves, states, err := replayOf(p)
		if err != nil || len(moves) != len(seqs)+1 || states[len(states)-1] != result {
			t.Fatal("replay:", len(moves), err)
		}
		for i, m := range moves {
			if m.Seq != i+2 {
				t.Fatalf("step %d of the replay is v%d", i, m.Seq)
			}
		}
	}
}

// sneaky is the counter, but "sneak" runs a hook while the host decides it.
type sneaky struct {
	counter.Game
	hook func()
}

func (g *sneaky) Decide(state []byte, actor string, cmd []byte) ([]byte, error) {
	if string(cmd) == "sneak" {
		if g.hook != nil {
			g.hook()
			g.hook = nil
		}
		cmd = []byte("inc")
	}
	return g.Game.Decide(state, actor, cmd)
}

// An entry that reaches the log between a round's draft and its entries, as
// another host's could, makes them stale: they are decided again, not
// acknowledged as if they had counted.
func TestARoundOvertakenByAnotherEntryIsDecidedAgain(t *testing.T) {
	g := &sneaky{}
	old := theGame
	theGame = g
	t.Cleanup(func() { theGame = old })
	ps := play(t, room.Config{Players: 3}, fast)
	r := ps[0].replica()
	data, _ := json.Marshal(room.Entry{Kind: room.KindGame, Peer: id(ps[1]), CommandID: "first", Body: []byte("+1"), Based: 1})
	g.hook = func() { r.Apply(data, time.Second) }
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if seq, err := ps[0].Submit(ctx, "second", []byte("sneak")); err != nil || seq != 3 {
		t.Fatal(seq, err)
	}
	converged(t, ps, "2")
}

// history is a game whose event is the whole new state: the commands so far.
// An event decided against a state the log never reached shows as a command
// lost or counted twice.
type history struct{}

func (history) Initial() []byte { return []byte("[]") }
func (history) Decide(state []byte, _ string, cmd []byte) ([]byte, error) {
	var cmds []string
	if err := json.Unmarshal(state, &cmds); err != nil {
		return nil, err
	}
	return json.Marshal(append(cmds, string(cmd)))
}
func (history) Apply(_, event []byte) ([]byte, error) { return event, nil }

// A round that times out while its entries are still on their way must not
// let the next round be drafted without them: its second entry would take the
// version of the first one's predecessor and be applied on the wrong state.
func TestARoundAfterATimedOutRoundWaitsForIt(t *testing.T) {
	old := theGame
	theGame = history{}
	t.Cleanup(func() { theGame = old })
	delayNetwork(t, 5*time.Millisecond)
	ps := play(t, room.Config{Players: 3}, fast)
	h, me := ps[0], id(ps[0])
	move := func(cmd string) *proposal {
		return &proposal{decide: func(d *room.Draft) (room.Entry, *room.Result, error) { return d.Decide(me, cmd, []byte(cmd)) }}
	}
	h.proposing.Lock()
	roundTimeout = time.Millisecond // a commit takes a 10 ms round trip
	first := h.agree([]*proposal{move("a")})
	h.agree([]*proposal{move("b"), move("c")})
	roundTimeout = timeout
	h.proposing.Unlock()
	if !first[0].Retry {
		t.Fatalf("the first round did not time out: %+v", first)
	}
	for _, cmd := range []string{"a", "b", "c"} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := h.Submit(ctx, cmd, []byte(cmd))
		cancel()
		if err != nil {
			t.Fatal(cmd, err)
		}
	}
	var cmds []string
	if err := json.Unmarshal(h.State(), &cmds); err != nil {
		t.Fatal(err)
	}
	if slices.Sort(cmds); !slices.Equal(cmds, []string{"a", "b", "c"}) {
		t.Fatalf("state %s does not hold each move exactly once", h.State())
	}
	converged(t, ps, string(h.State()))
}

func TestARoundDecidesAtMostMaxRound(t *testing.T) {
	ps := play(t, room.Config{Players: 3}, fast)
	h, me := ps[0], id(ps[0])
	r0, m0 := roundsOf(h)
	const n = maxRound + 44
	seqs := make([]int, n)
	var wg sync.WaitGroup
	h.proposing.Lock() // the requests queue up behind the round being served
	for i := range n {
		wg.Go(func() { seqs[i] = h.decide(me, fmt.Sprint("m", i), []byte("inc")).Seq })
	}
	waitFor(t, "every request queued", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.queue) == n
	})
	h.proposing.Unlock()
	wg.Wait()
	if r, m := roundsOf(h); r-r0 != 2 || m-m0 != n {
		t.Fatalf("%d requests took %d rounds of %d entries", n, r-r0, m-m0)
	}
	countedOnce(t, ps, seqs)
}
