# you-be-the-ball

A Go prototype for small, player-hosted game rooms on a LAN. One player creates a room and starts it once everyone is connected. From then on one player at a time is the host: it decides moves, and every player keeps an identical copy of the game in memory. If the host drops out of a game of three or more, another player takes over. The bundled counter is a test game, not part of the library.

## Offline demo

From the project root. The project needs Go 1.26; an older Go downloads it automatically, along with the modules, on first use:

```sh
go run ./cmd/demo
go test -race ./...
```

The demo runs three players (Alice hosts, Bob, Carol) in one process over 127.0.0.1. Alice is cut off, Bob or Carol takes over, and Bob's retried move returns its original receipt. It ends with `All players: counter=2 v4 finished; host changed from Alice to Bob` (or `Carol`).

## LAN quick start

Build the CLI:

```sh
go build -o bin/you-be-the-ball ./cmd/you-be-the-ball
```

Run each player in their own terminal or on their own machine. The binary is self-contained, so you can copy it to another machine with the same OS and architecture.

```sh
# Alice: host a room for three and leave it running. It prints an `Invite:` token.
./bin/you-be-the-ball host -players 3

# Bob and Carol: optionally look for open rooms, then join with the token.
./bin/you-be-the-ball find
./bin/you-be-the-ball join 'TOKEN'
```

Then type these commands into the running processes, in order:

| Where | Type | Expect |
| --- | --- | --- |
| Alice | `status`, then `start` | `phase=lobby online=3/3 …`, then `Started`. Only the host can start. Everyone prints `* game started`. |
| Bob | `inc bob-1` | `committed bob-1 at v2`. |
| Alice | `offline` | `Offline`, and a second later `* paused: no host reachable`. Within about a second Bob and Carol print `* host is now …`: `me` on the new host, the last 8 characters of its peer ID on the other. |
| Carol | `inc carol-1` | `committed carol-1 at v3`. |
| Bob | `inc bob-1` | `committed bob-1 at v2` again: the retry returns the original receipt. |
| Alice | `online`, then `status` | `* host is now …`, then `version=3 counter=2`, like the others. |
| Bob | `invite` | `Invite: …`, a token for a player whose program restarted (see Quitting). |
| New host | `finish` | `Finished`. Every player prints `* game finished; record saved to …`. `quit` exits. |

With two players only the creator hosts. If the guest goes offline, the host keeps playing and the guest catches up when it returns. If the host goes offline, the guest's moves fail and `status` shows `paused` until the host is back. If the host quits, the game ends for both: each records it as `interrupted`, and the guest prints `* room closed` (a guest that is offline at that moment stays paused until it quits too). A guest whose program restarted can rejoin (see Quitting); the creator cannot: once its program has closed, the game is over.

- **One machine:** give every player its own profile and pass `-listen /ip4/127.0.0.1/tcp/0` (and `-mdns=false` to the host), for example `join -profile carol -listen /ip4/127.0.0.1/tcp/0 'TOKEN'`.
- **Nothing found:** `find` browses for 8 seconds and lists only public lobbies that are not full. Otherwise paste the host's invite. The host listens on TCP 42042 by default; the other players on a random port.
- **Private rooms:** `host -private` is not advertised and needs the full invite, which carries a secret. Share it only with intended players.
- **Quitting and coming back:** with three or more players, a host that quits hands over to another player first. Nobody new can take a seat once the game has started, but a player who quit, or whose program restarted, comes back with `join -profile <the profile it played with>` (`host` for the creator by default) and the invite of a player whose program is still running: the original one while the creator's first program runs, otherwise a fresh one from any player's `invite` command (a player's invite goes stale when its program restarts). It prints `Rejoined at v…`. This needs a host, so most of the other players must be online, and it does not apply to the creator of a two-player game.

## Where game data is stored

By default, everything the CLI writes goes under `.you-be-the-ball` in your home directory (`%USERPROFILE%\.you-be-the-ball` on Windows), not in the project or the directory you launch from.

```text
~/.you-be-the-ball/
  config.json                 # optional defaults
  host/identity.key           # this player's peer identity
  host/games/<roomID>.json    # record of a game this player was in
  join/identity.key
  find/identity.key           # LAN browser identity
```

- **Profiles:** each subcommand uses the folder named after it, created on first use. The CLI prints `Data directory:` at startup. `-profile NAME` uses `~/.you-be-the-ball/NAME/` instead, for a second room or player on one machine. Run only one process per profile: in a started game a second one takes the seat from the first, which stops counting and shows `paused`, while the second may fail to rejoin. If another player is offline then, the game pauses for everyone until that player is back. To recover, stop both and rejoin once.
- **Nothing is written while playing.** Every player keeps the game in memory. Each player writes `games/<roomID>.json` in its own profile once, when the game finishes or expires, or with status `interrupted` when that player quits mid-game.
- **Reading a record:** `jq '{Status, Version, Players}' ~/.you-be-the-ball/host/games/*.json` shows the outcome; `.Events` is the full move list, and `jq -r '.State | @base64d'` shows the final game state. A record holds no invite secret.
- **Keep `identity.key` private.** It is the player's private key and stays across games. Delete a profile folder to start fresh.
- **Expiry:** a room lasts 72 hours from creation, or less with `host -ttl 10m`. After that no moves are accepted and the game is recorded as `expired`. Records are kept until you delete them.

To change the defaults, create `~/.you-be-the-ball/config.json` with only the fields you need:

```json
{"host_listen":"/ip4/0.0.0.0/tcp/42043", "players":2, "private":true}
```

`peer_listen` and `mdns` are also supported. Flags override the config file.

`-dir PATH` uses that folder instead of a profile. To keep the identity in a `.p2p-NAME` folder from an older build, stop that process and run `migrate PATH/.p2p-NAME` before you first use profile `NAME`. It moves the folder (same filesystem only) to `~/.you-be-the-ball/NAME` and never overwrites an existing profile. An old `room.json` in it is not used.

## What changed for players

- Every player, not only the host, must accept incoming connections: allow the program through your firewall, or give it a fixed port with `-listen /ip4/0.0.0.0/tcp/PORT` (or `peer_listen`) and open that port.
- Builds from before this change cannot join; they are told to upgrade.

## Packages

| Package | Responsibility |
| --- | --- |
| `game` | The `Game` interface: `Initial`, `Decide` and deterministic `Apply`. `game/counter` is the test game. To add a game, implement `game.Game` and point `play` in `cmd/you-be-the-ball/repl.go` at it. |
| `room` | The lobby (admission, private invites, start), the started game's state and its pure `Apply`, retry receipts, expiry, records. No network IO. |
| `lan` | libp2p identities, invites, mDNS discovery, the lobby protocol, and Raft replication (hashicorp/raft) of the game after the start. |
| `internal/home` | Profiles and `config.json` under `~/.you-be-the-ball`. |
| `cmd/you-be-the-ball` | The CLI. `cmd/demo` is the offline demo. |

`game` and `room` use only the standard library.

## Guarantees and limits

- **Retries:** use a fresh command ID for each action, and retry the same action with the same ID. A move that failed with `no host reachable` may still have counted; the retry returns its original receipt instead of counting twice, even after the game has ended. IDs are scoped to the player and room.
- **Agreement:** a move counts once a majority of the players hold it (with two players: once the host does). There are never two hosts whose moves can count. Every player sees the same state at the same version; a player can lag by a few milliseconds, or by up to about a second after being offline. A move returns once the mover's own copy holds it (or at most a second after it counted).
- **Hosts:** the creator hosts first. If the host drops out, the remaining players pick a random successor. With two players only the creator can host.
- **Limits:** 2–32 players, 4 KiB per game payload, 4,096 events per room including the start. There is no internet lobby, NAT traversal, relay or anti-cheat. This is not frame-by-frame action networking.

`DESIGN.md` describes the current design and the original proposal.
