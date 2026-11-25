# you-be-the-ball

A Go prototype for small, player-hosted game rooms on a LAN. One player hosts the room and starts it once everyone is connected. The host orders accepted changes; guests replay them. The bundled counter is a test game, not part of the library.

## Offline demo

From the project root, with Go 1.24 or newer:

```sh
go run ./cmd/demo
go test -race ./...
```

The demo runs a host and a guest over an in-memory pipe, including a disconnect and a retried command. It ends with `Host: counter=2 v3; guest: counter=2 v3`.

## LAN quick start

Build the CLI with Go 1.26 or newer. The first build downloads modules:

```sh
cd lan
go build -o ../bin/you-be-the-ball ./cmd/you-be-the-ball
cd ..
```

Run each player in their own terminal or on their own machine. The binary is self-contained, so you can copy it to another machine with the same OS and architecture.

```sh
# Alice: host a room and leave it running. It prints an `Invite:` token.
./bin/you-be-the-ball host -players 2

# Bob: optionally list open public rooms on the LAN, then join with the token.
./bin/you-be-the-ball find
./bin/you-be-the-ball join 'TOKEN'
```

Then type these commands into the running processes, in order:

| Where | Type | Expect |
| --- | --- | --- |
| Alice | `status`, then `start` | `online=2/2`, then `Started`. Only the host can start. |
| Bob | `inc bob-1`, then `offline` | `committed bob-1 at v2`. Bob stops syncing. |
| Alice | `inc alice-1` | `committed alice-1 at v3`. |
| Bob | `online`, then `inc bob-1` | Reconnected. The retry returns the original `v2` receipt. |
| Both | `status` | `version=3 counter=2` on both. `quit` exits. |

- **One machine:** pass `-listen /ip4/127.0.0.1/tcp/42042 -mdns=false` to `host`.
- **Nothing found:** `find` browses for 8 seconds and lists only public lobbies that are not full. Otherwise paste the host's invite. The host listens on TCP 42042 by default, so allow that port on your LAN.
- **Private rooms:** `host -private` is not advertised and needs the full invite, which carries a secret. Share it only with intended players.
- **Host quit:** nobody else can write. Run `host` again with the same profile and port, then type `online` on each guest.

## Where game data is stored

Everything the CLI writes goes under `.you-be-the-ball` in your home directory (`%USERPROFILE%\.you-be-the-ball` on Windows). Nothing is written to the project or to the directory you launch from.

```text
~/.you-be-the-ball/
  config.json          # optional defaults
  host/identity.key    # host peer identity
  host/room.json       # the game record: room ID, expiry, roster, committed events
  join/identity.key    # guest identity
  find/identity.key    # LAN browser identity
```

- **Profiles:** each subcommand uses the folder named after it, created on first use. The CLI prints `Data directory:` at startup. `-profile NAME` uses `~/.you-be-the-ball/NAME/` instead, for a second room or player on one machine. Run only one process per profile.
- **The game record lives only on the host.** Guests keep their copy in memory and rebuild it from the host when they rejoin.
- **Keep a profile to keep your place.** The host's folder resumes its room, and saved room settings win over creation flags. A guest's `identity.key` reclaims its seat once the game has started. Delete a profile folder to start fresh.
- **Keep it private.** The folder holds private keys and can hold a room's invite secret. To read the event log without the secret, run `jq '.Events' ~/.you-be-the-ball/host/room.json`.
- **Expiry:** a room lasts 72 hours from creation, or less with `host -ttl 10m`. Reconnecting does not extend it. An expired room refuses all operations and its `room.json` is deleted; `identity.key` is kept.

To change the defaults, create `~/.you-be-the-ball/config.json` with only the fields you need:

```json
{"host_listen":"/ip4/0.0.0.0/tcp/42043", "players":2, "private":true}
```

`peer_listen` and `mdns` are also supported. Flags override the config file.

`-dir PATH` stores a profile somewhere else; a relative path is resolved against the launch directory. Builds from before the home-directory layout wrote `.p2p-NAME` folders into the launch directory. `migrate PATH/.p2p-NAME` moves one to `~/.you-be-the-ball/NAME`; stop the old process first. It never overwrites an existing profile.

## Packages

| Package | Responsibility |
| --- | --- |
| `room` | Admission and start, fixed roster after start, private invites, event ordering, retry receipts, atomic host storage, expiry. |
| `session` | Versioned request/response stream, bounded frames, paged replay, guest replica. Browsing never takes a seat. |
| `lan` | libp2p identities and streams, invites, mDNS discovery. `lan/cmd/you-be-the-ball` is the CLI. |
| `example/counter` | The three game hooks: `Initial`, `Decide` and deterministic `Apply`. Replace these to plug in another game. |

`lan` is a separate module so the core and its offline tests stay free of networking dependencies.

## Guarantees and limits

- **Retries:** use a fresh command ID for each action, and retry the same action with the same ID. IDs are scoped to the player and room.
- **Consistency:** every replica has the same state at the same event version. Guests can lag; the CLI polls every half second, and a broken connection can take up to 20 seconds to notice. This is not frame-by-frame action networking.
- **Limits:** 2–32 players, 4 KiB per game payload, 4,096 events per room including the start. There is no host migration, internet lobby, NAT traversal, relay or anti-cheat.

`DESIGN.md` describes the intended next stage.
