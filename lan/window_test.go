package lan

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/kaobei-alabo/you-be-the-ball/room"
)

// The host commits one move but its answer is lost, and only once the mover's
// other Submits have committed all they can: room.Receipts of them would push
// the move's receipt out. Its retry must still count once.
func TestARetryCountsOnceWhileThePlayerKeepsMoving(t *testing.T) {
	ps := play(t, room.Config{Players: 2}, fast)
	h, g := ps[0], ps[1]
	held := make(chan struct{})
	var taken atomic.Bool
	h.n.h.SetStreamHandler(lobbyProto, func(s network.Stream) {
		var q request
		if recv(s, &q) != nil || q.Op != "submit" {
			_ = s.Reset()
			return
		}
		r := h.serveSubmit(s.Conn().RemotePeer(), q)
		if q.ID == "slow" && taken.CompareAndSwap(false, true) {
			v0, v, still := h.version(), h.version(), time.Now()
			for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
				if n := h.version(); n != v {
					v, still = n, time.Now()
				}
				if v-v0 > room.Receipts+64 || time.Since(still) > time.Second {
					break
				}
			}
			t.Logf("%d moves committed while the answer was held", v-v0)
			close(held)
			_ = s.Reset() // the answer is lost
			return
		}
		_ = send(s, r)
		_ = s.Close()
	})
	var acked atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 32 {
		wg.Go(func() {
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				_, err := g.Submit(ctx, fmt.Sprint("w", w, "-", n), []byte("inc"))
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
				acked.Add(1)
			}
		})
	}
	waitFor(t, "some moves", func() bool { return acked.Load() > 100 })
	seq := inc(t, g, "slow")
	<-held
	close(stop)
	wg.Wait()
	total := int(acked.Load()) + 1
	waitFor(t, "the players agree", func() bool { return string(h.State()) == string(g.State()) && h.version() == g.version() })
	if got := string(h.State()); got != strconv.Itoa(total) {
		t.Fatalf("counter %s after %d acknowledged moves", got, total)
	}
	finish(t, ps, strconv.Itoa(total))
	ms, _, err := replayOf(ps[0])
	if err != nil {
		t.Fatal(err)
	}
	var seqs []int
	for _, m := range ms {
		if m.ID == "slow" {
			seqs = append(seqs, m.Seq)
		}
	}
	if len(seqs) != 1 || seqs[0] != seq {
		t.Fatalf("the move acknowledged at v%d was applied at %v", seq, seqs)
	}
}

func TestWindowDoesNotHoldUpNormalUse(t *testing.T) {
	ps := play(t, room.Config{Players: 2}, fast)
	g := ps[1]
	var wg sync.WaitGroup
	for w := range 64 {
		wg.Go(func() {
			for n := range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				_, err := g.Submit(ctx, fmt.Sprint("w", w, "-", n), []byte("inc"))
				cancel()
				if err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	converged(t, ps, "1280")

	// A full window: the next Submit waits until its context ends or the player closes.
	for range ahead {
		if _, err := g.window.enter(context.Background(), g.done); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if _, err := g.Submit(ctx, "late", []byte("inc")); !errors.Is(err, context.DeadlineExceeded) || time.Since(begin) > 5*time.Second {
		t.Fatal("a waiting Submit whose context ended:", err, time.Since(begin))
	}
	done := make(chan error)
	go func() {
		_, err := g.Submit(context.Background(), "waits", []byte("inc"))
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // let it wait
	g.Close()
	select {
	case err := <-done:
		if !errors.Is(err, errClosed) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close left a Submit waiting")
	}
	if s := ps[0].State(); string(s) != "1280" {
		t.Fatal("a move that waited counted:", string(s))
	}
}

// Tickets let in out of order still release the window in order.
func TestWindowLetsTicketsInInOrder(t *testing.T) {
	var w window
	for i := range ahead {
		if n, _ := w.enter(context.Background(), nil); n != i {
			t.Fatal(n)
		}
	}
	in := make(chan int, 2)
	for range 2 {
		go func() {
			n, _ := w.enter(context.Background(), nil)
			in <- n
		}()
	}
	waitFor(t, "two waiting", func() bool { w.mu.Lock(); defer w.mu.Unlock(); return len(w.waiting) == 2 })
	w.leave(1) // not the oldest: nothing moves
	select {
	case n := <-in:
		t.Fatal("let in early:", n)
	case <-time.After(50 * time.Millisecond):
	}
	w.leave(0) // the oldest goes, and 1 with it: both may enter
	a, b := <-in, <-in
	if a+b != 2*ahead+1 || w.oldest != 2 || len(w.finished) != 0 || len(w.waiting) != 0 {
		t.Fatal(a, b, w.oldest, w.finished, w.waiting)
	}
}
