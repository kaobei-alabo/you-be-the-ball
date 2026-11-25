// Package game defines what a game supplies to a room. The room orders and
// replicates events; only the game knows what they mean.
package game

// Game callbacks must be quick and must not re-enter the room. Apply must be
// deterministic: every player's copy runs it on the same events in the same order.
// Decide runs only on the host, so it may use a clock or a die as long as the
// chosen value is recorded in the event it returns.
type Game interface {
	Initial() []byte
	Decide(state []byte, actor string, command []byte) ([]byte, error)
	Apply(state []byte, event []byte) ([]byte, error)
}

// Finisher is optional. A game that implements it ends itself: once Finished
// reports true the room accepts no more commands. It must be deterministic.
type Finisher interface {
	Finished(state []byte) bool
}
