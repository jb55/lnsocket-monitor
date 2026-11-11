# lnsocket-monitor

Watch a Core Lightning node over [commando](https://docs.corelightning.org/reference/lightning-commando)
with [lnsocket-rs](https://github.com/jb55/lnsocket-rs) and print interesting
events to stdout as JSON lines. `lnsocket-nudge` turns them into macOS
notifications with [nudge](https://github.com/jb55/nudge).

```sh
cargo build --release
./lnsocket-nudge                  # run the monitor and post notifications
./lnsocket-nudge -- -n mynode --peers
target/release/lnsocket-monitor --replay 5 | ./lnsocket-nudge --stdin --dry-run
```

## Node auth

The node comes from the `lnrpc` config (`lnrpc config add <name> <nodeid> <host> <rune>`):
`-c <path>`, `$LNRPC_CONFIG`, `$XDG_CONFIG_HOME/lnrpc/config.toml` or
`~/.config/lnrpc/config.toml`, and `-n <name>` picks a node other than the
default. The rune needs `getinfo`, `wait`, `listinvoices`, `listsendpays`,
`listforwards`, `listchainmoves`, `listnodes`, `listpeerchannels` and
`listclosedchannels` (plus `listnetworkevents` for `--peers`).

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

`--replay N` also reports the last N updates of each kind at startup, for testing.

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

## lnsocket-nudge

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

Chain events open the transaction on mempool.space. `--skip forward_settled,peer_connected` drops
event types. The monitor is restarted if it exits.
