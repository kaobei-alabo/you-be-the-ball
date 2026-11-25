# you-be-the-ball

A Go prototype for small, player-hosted rooms. One player creates the room and starts it when the required players are connected. The host orders accepted changes; guests replay them. The shared counter is a test game, not a dependency of the library.

## See sync work without a network

From the project root, with Go 1.24 or newer:

```sh
go run ./cmd/demo
go test -race ./...
```

Alice hosts; Bob joins, increments, and disconnects. Alice increments while he is away. Bob reconnects and retries his original command. The expected final line is **`Host: counter=2 v3; guest: counter=2 v3`**: one start event and two distinct increments. This uses the same framed session protocol as the network adapter but runs over `net.Pipe`. It does not test discovery or real sockets.

## Try two terminals or two laptops

On each machine, in `lan/`, use Go 1.26.x (or a newer compatible Go toolchain). This separate module keeps the core and offline tests free of networking dependencies. First setup needs access to the Go module proxy:

```sh
cd lan
go mod tidy
go test -race ./...           # includes TestLoopbackCounter over real TCP/libp2p
mkdir -p ../bin
go build -o ../bin/you-be-the-ball ./cmd/you-be-the-ball
```

If you have existing `.p2p-*` data you want to keep, **migrate it before starting the new binary** (below). For a new game, run the following from the project root in separate terminals. You can also copy just the binary to another machine of the same OS/architecture; the binary does not look for a checkout when it starts:

```sh
# Terminal A: Alice's machine. Leave this running.
./bin/you-be-the-ball host -players 2

# Terminal B: Bob's machine. Optional: discover open public rooms on this LAN.
./bin/you-be-the-ball find

# Then join. Replace TOKEN with the full token from `find` or Alice's `Invite:` line.
./bin/you-be-the-ball join 'TOKEN'
```

Type the following commands into the running processes, in order:

| Where | Type | What to look for |
| --- | --- | --- |
| Alice | `status`, then `start` | `online=2/2`, then `Started`. A guest cannot start. |
| Bob | `inc bob-1`, then `offline` | The receipt is `v2`. Bob stops syncing. |
| Alice | `inc alice-1` | The host reaches `v3`. |
| Bob | `online`, then `inc bob-1` | Reconnected; retry gets the original `v2` receipt. |
| Both | `status` | `version=3 counter=2` on both. Use `quit` to exit. |

- Reuse the **same guest data directory** when relaunching to keep the authenticated identity and reclaim a locked seat. Reconnections do not extend a room's expiry.
- Public discovery only lists non-full lobbies. A private host (`host -private`) is not advertised and requires its invite secret. Give the entire private invite only to intended players.
- If `find` is empty, try the printed invite directly. The default host listens on TCP 42042. Allow that port on your trusted LAN; guest-network isolation can block even direct connections. For a single-machine test pass `-listen /ip4/127.0.0.1/tcp/42042 -mdns=false` to `host`.
- If the host quits, no one else can write. Restart `host` with the same directory and listen port, then type `online` on guests. An unexpired room is resumed; creation flags do not override its settings. Use a fresh profile for a new room. Never share one profile between live processes.

## Where configuration and state live

The installed CLI uses the current user's home directory, **not the launch directory**. On Windows the root is `%USERPROFILE%\.you-be-the-ball`. Each machine uses its own files:

```text
~/.you-be-the-ball/
  config.json          # optional settings, read at startup
  host/identity.key    # stable host peer identity
  host/room.json       # room ID, expiry, roster, committed events
  join/identity.key    # guest identity for reconnecting
  find/identity.key    # LAN browser identity
```

Profiles are created on use; the CLI prints the selected directory. Guest game replicas stay in memory and are rebuilt from the host when you rejoin. You still need an invite to join. Do not share the state folder: it contains private keys and can contain private room secrets. To inspect the default host's game log without printing its invite secret, run `jq '.Events' ~/.you-be-the-ball/host/room.json`.

No config file is required. To customize defaults, create `~/.you-be-the-ball/config.json` with just the fields you want, for example:

```json
{"host_listen":"/ip4/0.0.0.0/tcp/42043", "players":2, "private":true}
```

`peer_listen` and `mdns` are also supported. Explicit flags override config; the room's saved settings still win on resume. `-profile colleague-test` selects `~/.you-be-the-ball/colleague-test/`, useful for a separate room on the same laptop. The old `-dir PATH` is an explicit override (relative to the current directory if not absolute); don't pass it to use home storage.

### Move a room from the old CLI

Stop all old host/guest processes (`quit`) **before** moving their directories. From the project root, if you ran the old CLI from `lan/`:

```sh
./bin/you-be-the-ball migrate lan/.p2p-*
```

This explicitly moves each named directory, e.g. `lan/.p2p-host` → `~/.you-be-the-ball/host` and `lan/.p2p-colleague-test` → `~/.you-be-the-ball/colleague-test`. It preserves keys, events and expiration; it never merges or overwrites an existing profile. If you kept the old files somewhere else, pass their actual paths. Migration is a same-filesystem move, and startup never adopts data from a random working directory. Resume the old default room with `host`; resume the colleague room with `host -profile colleague-test -listen /ip4/0.0.0.0/tcp/42043` if that was its old port. The wire protocol and discovery name are unchanged, so an existing colleague binary need not upgrade just to connect.

## Where the core ends

| Package | Responsibility |
| --- | --- |
| `room` | Serialized admission and start, fixed roster after start, private invites, event ordering, retry receipts, atomic host storage, expiry. |
| `session` | Versioned request/response stream, bounded frames, paged replay, guest replica. Browsing never takes a seat. The transport supplies authenticated identities. |
| `lan` | libp2p identities and streams, manual invites and mDNS discovery; `lan/cmd/you-be-the-ball` is the interactive harness. |
| `example/counter` | The three game hooks: `Initial`, `Decide`, and deterministic `Apply`. Replace these to integrate another game. |

Use a fresh command ID for each logical action; retry **the same action** with the same ID. IDs are scoped to the requesting player and room. Distinct players may use the same ID. Clients apply accepted events in sequence and can lag; the guarantee is the same shared state **at the same event version**. Guests poll every half second in the CLI. A broken connection may take up to the stream deadline (20 s) to be noticed. This is not frame-by-frame action-game networking.

The host stores `room.json` in its data directory; the fixed deadline defaults to 72 hours **from creation**, with shorter TTLs supported (`host -ttl 10m`). Expired rooms refuse all further operations; the running host removes room data and guests discard in-memory replicas. If the host was shut down, its data is purged before hosting from that directory again. The node identity is separate and intentionally survives room expiry. The core cannot erase another player's independent copy.

**Prototype limits:** 2–32 players, 4 KiB per game payload, 4,096 events per room including start. The host rewrites a bounded log on each commit; there is no compaction or host migration. There is no internet lobby service, NAT traversal, relay, or anti-cheat. Test the LAN flow before building those layers. `DESIGN.md` describes the intended next stage.
