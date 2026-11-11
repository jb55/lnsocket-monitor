# lnsocket-monitor

Watch a Core Lightning node over [commando](https://docs.corelightning.org/reference/lightning-commando)
with [lnsocket-rs](https://github.com/jb55/lnsocket-rs) and print interesting
events to stdout as JSON lines: payments received and sent, forwards,
on-chain deposits, channels opened and closed. [`contrib/nudge`](contrib/nudge)
turns them into macOS notifications with [nudge](https://github.com/jb55/nudge).

Site: https://jb55.github.io/lnsocket-monitor/

```sh
cargo install --git https://github.com/jb55/lnsocket-monitor
lnsocket-monitor                           # the default node from your lnrpc config
lnsocket-monitor -n mynode --peers | jq .
lnsocket-monitor --replay 5                # also print the last 5 of each kind, to try it out
```

## Node auth

The node comes from the [`lnrpc`](https://github.com/jb55/lnsocket-rs#lnrpc)
config: `-c <path>`, `$LNRPC_CONFIG`, `$XDG_CONFIG_HOME/lnrpc/config.toml` or
`~/.config/lnrpc/config.toml`, and `-n <name>` picks a node other than the
default. Add one with:

```sh
lnrpc config add mynode <nodeid> <host[:port]> <rune>
```

A read-only rune is enough. On the node:

```sh
lightning-cli createrune restrictions='[["method^list","method^get","method=wait"],["method/listdatastore"]]'
```

The calls it makes are `getinfo`, `wait`, `listinvoices`, `listsendpays`,
`listforwards`, `listchainmoves`, `listnodes`, `listpeerchannels` and
`listclosedchannels`, plus `listnetworkevents` for `--peers`. A node too old
to `wait` on a subsystem (`chainmoves` and `networkevents` are recent) has it
skipped, with a note on stderr. It was built against v26.06.

## Events

Every line has `type`, `node` (the config name) and `time`. Amounts are msat.

| type | from | notable fields |
|---|---|---|
| `monitor_started` | `getinfo` | `id`, `alias`, `version` |
| `invoice_paid` | invoices | `amount_received_msat`, `label`, `description` (a zap request for zaps) |
| `payment_sent` | sendpays | `amount_msat`, `fee_msat`, `destination_alias`, `parts` |
| `forward_settled` | forwards | `in_alias`, `out_alias`, `out_msat`, `fee_msat` |
| `onchain_deposit` | chainmoves | `amount_msat`, `txid` |
| `channel_opened` | chainmoves | `peer_alias`, `capacity_msat`, `opener` |
| `channel_closed` | chainmoves | `peer_alias`, `capacity_msat`, `to_us_msat`, `closing_txid` |
| `peer_connected`, `peer_disconnected` | networkevents, with `--peers` | `peer_alias` (channel peers only) |

```json
{"type":"invoice_paid","node":"mynode","time":1794390955,"label":"coffee","amount_msat":21000,"amount_received_msat":21000,"payment_hash":"…","bolt12":false}
```

## How it works

Nothing is polled. Each subsystem is watched with CLN's `wait`, which
returns when the node records something new; the monitor then reads what
changed with the matching `list*` call from the last index it saw, so a
reconnect never skips anything. Channel opens and closes come from chain
moves, recorded when the funding is locked in and when it is spent.

A commando connection answers nothing else while a command runs on it, so
each `wait` gets its own connection (a bare `LNSocket` with a throwaway key),
which pings the node every minute and reconnects when it stops hearing back.
The `list*`, `listnodes` and heartbeat calls share one `CommandoClient`.

This needs lnsocket-rs's `fixes` branch, where `read_custom` is cancel-safe;
on master a request sent while a reply is half read desyncs the stream.

## contrib/nudge: macOS notifications

[`contrib/nudge/lnsocket-nudge`](contrib/nudge/lnsocket-nudge) runs
`lnsocket-monitor` and posts each event with
[nudge](https://github.com/jb55/nudge) (`brew install --cask jb55/nudge/nudge`).
It's one Python file, standard library only.

```sh
contrib/nudge/lnsocket-nudge                          # run the monitor, post notifications
contrib/nudge/lnsocket-nudge -- -n mynode --peers     # arguments after -- go to the monitor
lnsocket-monitor --replay 5 | contrib/nudge/lnsocket-nudge --stdin --dry-run
```

It uses `lnsocket-monitor` from your PATH, or the one built in this checkout,
and restarts it if it exits. `--skip forward_settled,peer_connected` drops
event types.

Zaps are looked up on a local nostr relay (notedeck's, `ws://127.0.0.1:6677`
by default; `--relay ''` turns it off) for the zapper's name and avatar and
the zapped note. Clicking a zap opens what was zapped (the note, or the
profile for a profile zap) in notedeck with `nd call open nostr:…`, through
this entry in `~/.config/nudge/actions.json`:

```json
"lnsocket-open": {
  "label": "Open in Notedeck",
  "command": ["/bin/bash", "-lc", "exec nd call open \"$NOSTR_URI\""]
}
```

Chain events open the transaction on mempool.space.

Every notification carries an icon so it reads as the node's, not nudge's:
the zapper's picture on a zap, and otherwise a bolt drawn once into
`~/Library/Caches/lnsocket-nudge/`, with the node's alias as the sender
(`--icon PATH` for your own, `--icon ''` for none). macOS shows the sender
and moves the title into the body of a notification with an icon.
