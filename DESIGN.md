# you-be-the-ball: a player-hosted room for Go games

## Current design

What is implemented now (see README.md for how to use it):

- **Lobby on the creator.** Before the start, only the creator's program holds the room; guests take seats over the lobby protocol.
- **Raft after the start.** The ordered event log is replicated with Raft (github.com/hashicorp/raft v1.8.0), in memory on every player. The host is the elected leader. A move counts once a majority of voters hold it, so there are never two hosts whose moves can count.
- **Random successor.** Raft's randomized election timeouts pick the next host when the current one drops out; a host that quits hands over to a random other voter first (with two players there is none). The creator is the first host.
- **Two players.** Only the creator votes: if the guest drops, the host plays on and the guest catches up; if the host drops, the game pauses; if the host's program closes, the game ends as `interrupted` for both.
- **One replication address per run.** Each run of the program uses `<peerID>/<roomID>/<nonce>` as its Raft address, so a restarted program with an empty memory cannot be mistaken for its old self.
- **Rejoining after a restart.** A restarted program joins again with the same identity and a new replication address. Any member tells it where the host is; only the host sends it the Setup (a different one from a later answer fails the rejoin) and where the other members are now. The host removes the old run's Raft server, adds the new one as a nonvoter and, once it holds the host's version, makes it a voter again if its seat votes. This needs a host.
- **A record at the end.** Each player writes `games/<roomID>.json` once, when the game finishes or expires or when it leaves mid-game. Nothing is written per move.
- **One game per build.** A second selectable game needs a game name in `room.Config` and `Setup`, checked on join.

These statements in the proposal below no longer hold:

- A move is durable because a majority of players hold it in memory; nothing is written per move.
- Hosts change: if the host drops out, the others elect one (not with two players). No game resumes from disk: a player whose program restarted gets its seat back from the other players' memory, through the host.
- Records are kept until the user deletes them. The proposal purged everything at expiry; whether records should be purged too is an open question.

## Original design proposal

See README.md for the implemented prototype, its limits, and the offline demo. The first target is a small room of native Go peers with one trusted player hosting. The core owns membership and replication of shared state; the game owns the meaning of a move. No dedicated game server is required. The first milestone is deliberately limited to a LAN.

## What a game can ask for

- Create a room with a required player count, including the creator.
- Find advertised rooms nearby, or join a specific room using an invite.
- See the current roster and whether the host can start. Filling the room enables Start; it does not start the game.
- Start as the creator, submit a command, observe committed events, and reconnect to the same room.

The host handles join, disconnect, and start requests one at a time. Start succeeds only in the lobby, from the creator, with the required number of distinct admitted peers currently connected. A successful start records the final roster. A returning member can reconnect after start; a new player cannot take their place. Use the authenticated peer ID, not a display name or a client-supplied sender field. A node must retain its identity across restarts to reclaim its seat.

## Finding a host is different from reaching it

| Environment | Find the host | Reach the host |
| --- | --- | --- |
| First test, two local processes | Copy an invite containing the host peer ID and a dialable address | Open a go-libp2p stream to that peer |
| Same LAN | Use application-specific mDNS to discover cores; query them for advertised lobbies | Connect to the selected host; also allow a pasted address when multicast is unavailable |
| Across the internet, later | Look up a room invite in a small public directory for the host ID and addresses | Use a direct connection when possible. For private networks, provision a public circuit relay; libp2p can try to upgrade a relayed connection to a direct one using hole punching |

Discovery is not admission: finding a peer must not add it to a room. For private rooms, carry an unguessable join secret in the invitation, not the LAN advertisement. The internet directory should expire stale listings and the relay should forward traffic without owning room state. A public room browser, distributed discovery network, and full mesh are not MVP requirements.

## Agreement and recovery

Give each room one writer: its host. Each player's core, including the host's own player, submits commands. The host checks membership and room phase, then asks the game to validate any game-specific command. On acceptance, it durably records the resulting event(s) with consecutive room sequence numbers before acknowledging or broadcasting. Deduplicate retries by `(room, authenticated peer, command ID)` and return the original result; record the deduplication result atomically with the events.

All peers apply only host-committed events, in order and once per sequence number. The core understands its lobby events, but game events are opaque payloads. A game supplies a deterministic `Apply(state, event)` function. If a decision uses a clock or a die roll, record the chosen value in the event rather than having peers generate it independently. Only shared state belongs in this log.

A disconnected guest stays at the last committed version. On reconnect it supplies that version; the host returns the missing events, or a versioned snapshot followed by subsequent events. For example, a peer at version 5 that sees version 7 must recover version 6 before applying 7. Replicas can lag; the promise is the same state **at the same version**, not simultaneous updates. If the host disappears, nobody else accepts writes. The same host can resume from its persisted identity and log before expiry; there is no host migration in v1.

## Retention

Proposed interpretation of “at most three days”: a room has one immutable deadline, at most 72 hours after creation. Reconnects, snapshots, and restarts do not extend it. Stop serving expired rooms, and purge local events, snapshots, and retry records at expiry while running or before serving anything on the next startup. A powered-off device cannot run cleanup, and independently controlled peers can copy their data; the core cannot promise erasure from someone else's device.

## Build order and proof

1. **Room rules, without a network:** serialize all decisions within each hosted room (the prototype uses a mutex). Test two peers racing for the last slot, early and non-host starts, and a retried request. Use an injectable clock to test expiry without waiting three days.
2. **A real LAN session:** create a tiny CLI using go-libp2p streams. Connect two terminals by address, then add mDNS and try two laptops. The only test payload needed is a counter increment. Disconnect a guest, increment on the host, and reconnect; both peers must report the same counter and version, even after a retried increment.
3. **Internet reachability:** test across two different home networks, then deliberately require the relay. Verify the same create/join/start/replay behavior without changing the room protocol. Note in diagnostics whether a connection is direct or relayed.

This design covers lobby and discrete shared-state updates. High-rate movement, prediction, rollback, hidden player state, and competitive anti-cheat are separate game-dependent requirements.
