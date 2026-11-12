//! `lnsocket-monitor`: watch a Core Lightning node over commando and print
//! interesting events to stdout, one JSON object per line.
//!
//! ```text
//! lnsocket-monitor [-c <config>] [-n <node>] [--replay <n>] [--peers]
//! ```
//!
//! The node comes from the `lnrpc` config file (`-c`, `$LNRPC_CONFIG`,
//! `$XDG_CONFIG_HOME/lnrpc/config.toml` or `~/.config/lnrpc/config.toml`),
//! the same `[nodes.<name>]` entries `lnrpc config add` writes.
//!
//! Nothing is polled for events. Invoices, sendpays, forwards and chain moves
//! (deposits, channel opens and closes), plus network events with `--peers`,
//! are watched with CLN's `wait` RPC, which returns as soon as the node
//! records something new.
//!
//! A commando connection answers nothing else while a command is running on
//! it, so a blocking `wait` gets a connection to itself: each subsystem has a
//! [`wait_loop`] task driving a bare [`LNSocket`], which pings the node every
//! minute and reconnects if it hears nothing back. When a `wait` returns, the
//! task hands the new index to the main loop, which reads what changed with
//! the matching `list*` call over one shared [`CommandoClient`]. That client
//! also sends a `getinfo` every minute, so a dead connection is noticed and
//! reconnected by lnsocket.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lnsocket::bitcoin::secp256k1::{PublicKey, SecretKey, rand};
use lnsocket::commando::{
    CallOpts, CommandoCommand, CommandoConfig, IncomingCommandoMessage, RetryPolicy,
    read_incoming_commando_message,
};
use lnsocket::ln::msgs::{Ping, Pong};
use lnsocket::ln::wire::Message;
use lnsocket::{CommandoClient, Error, LNSocket};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

const USAGE: &str = "\
usage: lnsocket-monitor [-c <config>] [-n <node>] [--replay <n>] [--peers] [-v]

  -c, --config <path>   lnrpc config file (default: $LNRPC_CONFIG or ~/.config/lnrpc/config.toml)
  -n, --node <name>     node from the config (default: its `default`)
  --replay <n>          also report the last <n> updates of each kind on startup (for testing)
  --peers               also report channel peers connecting and disconnecting. this
                        wakes on every network event the node logs, pings included
  -v, --verbose         also log heartbeats and network event catch-ups

events go to stdout. the log goes to stderr in logfmt: ts, level (debug, info,
warn), src=monitor, msg, then fields.

prints one JSON object per line: monitor_started, invoice_paid, payment_sent,
forward_settled, onchain_deposit, channel_opened, channel_closed, and with
--peers, peer_connected and peer_disconnected.";

/// Calls other than `wait` give up after this long, so a lost reply can't
/// stall the loop forever.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// How often each connection checks it is still alive.
const HEARTBEAT: Duration = Duration::from_secs(60);

/// The channel peer list is refetched for an unknown id at most this often.
const PEER_REFRESH: Duration = Duration::from_secs(600);

#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    nodes: BTreeMap<String, Node>,
}

#[derive(Debug, Clone, Deserialize)]
struct Node {
    nodeid: String,
    host: String,
    rune: String,
}

impl Node {
    fn addr(&self) -> String {
        if self.host.contains(':') {
            self.host.clone()
        } else {
            format!("{}:9735", self.host)
        }
    }
}

struct Opts {
    config: Option<PathBuf>,
    node: Option<String>,
    replay: u64,
    peers: bool,
    verbose: bool,
}

/// A CLN `wait` subsystem and the `list*` call that reads what changed.
struct Watch {
    subsystem: &'static str,
    indexname: &'static str,
    list: &'static str,
    /// The array in the list result.
    field: &'static str,
}

const INVOICES: Watch = Watch {
    subsystem: "invoices",
    indexname: "updated",
    list: "listinvoices",
    field: "invoices",
};
const SENDPAYS: Watch = Watch {
    subsystem: "sendpays",
    indexname: "updated",
    list: "listsendpays",
    field: "payments",
};
const FORWARDS: Watch = Watch {
    subsystem: "forwards",
    indexname: "updated",
    list: "listforwards",
    field: "forwards",
};
const CHAINMOVES: Watch = Watch {
    subsystem: "chainmoves",
    indexname: "created",
    list: "listchainmoves",
    field: "chainmoves",
};
const NETWORKEVENTS: Watch = Watch {
    subsystem: "networkevents",
    indexname: "created",
    list: "listnetworkevents",
    field: "networkevents",
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let Some(opts) = parse_args(std::env::args().skip(1).collect())? else {
        println!("{USAGE}");
        return Ok(());
    };
    let path = match &opts.config {
        Some(p) => p.clone(),
        None => default_config_path()?,
    };
    let config = load_config(&path)?;
    let name = opts.node.clone().or(config.default.clone()).ok_or_else(|| {
        format!(
            "no node given and no default in {}; add one with `lnrpc config add`",
            path.display()
        )
    })?;
    let node = config
        .nodes
        .get(&name)
        .ok_or_else(|| format!("no node named '{name}' in {}", path.display()))?
        .clone();
    let pubkey = PublicKey::from_str(&node.nodeid)
        .map_err(|e| format!("invalid nodeid '{}': {e}", node.nodeid))?;
    VERBOSE.store(opts.verbose, Ordering::Relaxed);

    let calls = connect_calls(&node, pubkey).await;
    let mut mon = Monitor {
        name,
        node,
        pubkey,
        replay: opts.replay,
        calls,
        seen: HashMap::new(),
        sent: HashSet::new(),
        peers: Peers::default(),
        aliases: HashMap::new(),
        emitted: 0,
    };
    mon.start().await;

    let mut watches = vec![&INVOICES, &SENDPAYS, &FORWARDS, &CHAINMOVES];
    if opts.peers {
        watches.push(&NETWORKEVENTS);
    }
    let (tx, mut rx) = mpsc::unbounded_channel();
    for w in watches {
        tokio::spawn(wait_loop(mon.node.clone(), pubkey, w, tx.clone()));
    }
    drop(tx);

    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.tick().await; // the first tick is immediate
    loop {
        tokio::select! {
            changed = rx.recv() => match changed {
                Some((w, cur)) => mon.catch_up(w, cur).await,
                None => return Err("no subsystems left to watch".into()),
            },
            _ = heartbeat.tick() => match mon.call("getinfo", json!({})).await {
                Ok(_) => log!(Debug, "heartbeat"),
                Err(e) => log!(Warn, "heartbeat failed", err = e),
            },
        }
    }
}

fn parse_args(mut args: Vec<String>) -> Result<Option<Opts>, String> {
    let mut opts = Opts {
        config: None,
        node: None,
        replay: 0,
        peers: false,
        verbose: false,
    };
    while !args.is_empty() {
        let flag = args.remove(0);
        let mut value = || {
            if args.is_empty() {
                Err(format!("{flag} needs a value\n\n{USAGE}"))
            } else {
                Ok(args.remove(0))
            }
        };
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "-c" | "--config" => opts.config = Some(PathBuf::from(value()?)),
            "-n" | "--node" => opts.node = Some(value()?),
            "--replay" => {
                let v = value()?;
                opts.replay = v.parse().map_err(|_| format!("bad --replay '{v}'"))?;
            }
            "--peers" => opts.peers = true,
            "-v" | "--verbose" => opts.verbose = true,
            other => return Err(format!("unknown argument '{other}'\n\n{USAGE}")),
        }
    }
    Ok(Some(opts))
}

fn default_config_path() -> Result<PathBuf, String> {
    if let Some(p) = std::env::var_os("LNRPC_CONFIG") {
        return Ok(PathBuf::from(p));
    }
    let base = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(x) if !x.is_empty() => PathBuf::from(x),
        _ => PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?).join(".config"),
    };
    Ok(base.join("lnrpc").join("config.toml"))
}

fn load_config(path: &Path) -> Result<Config, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// A fresh connection under a throwaway node key. Retries until it works.
async fn connect_sock(node: &Node, pubkey: PublicKey, what: &str) -> LNSocket {
    let addr = node.addr();
    let mut backoff = Duration::from_secs(1);
    loop {
        let key = SecretKey::new(&mut rand::thread_rng());
        match LNSocket::connect_and_init(key, pubkey, &addr).await {
            Ok(sock) => {
                log!(Info, "connected", conn = what, addr = addr);
                return sock;
            }
            Err(e) => {
                let retry = format!("{}s", backoff.as_secs());
                log!(Warn, "connect failed", conn = what, addr = addr, err = e, retry_in = retry);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

/// The connection for everything except `wait`.
async fn connect_calls(node: &Node, pubkey: PublicKey) -> Arc<CommandoClient> {
    let sock = connect_sock(node, pubkey, "rpc").await;
    // Everything sent here is read-only, so it is resent across reconnects
    // for as long as it takes. The timeout is per call, in `Monitor::call`.
    let config = CommandoConfig::new()
        .timeout(None)
        .reconnect(usize::MAX, Duration::from_secs(1), Duration::from_secs(60))
        .retry_policy(RetryPolicy::Always {
            max_retries: usize::MAX,
        });
    Arc::new(CommandoClient::spawn_with_config(
        sock,
        node.rune.as_str(),
        config,
    ))
}

/// Why a `wait` didn't produce an index.
enum WaitError {
    /// The node refused it, eg. a subsystem it doesn't have. Retrying won't help.
    Rpc(String),
    /// The connection broke or went quiet; reconnect and send it again.
    Conn(String),
}

/// Keep one `wait` in flight for `w` on a connection of its own, sending
/// every index it returns to the main loop.
async fn wait_loop(
    node: Node,
    pubkey: PublicKey,
    w: &'static Watch,
    tx: mpsc::UnboundedSender<(&'static Watch, u64)>,
) {
    // nextvalue 0 returns straight away with where the index is now.
    let mut next = 0;
    let mut id = 0;
    loop {
        let mut sock = connect_sock(&node, pubkey, w.subsystem).await;
        loop {
            id += 1;
            match wait_once(&mut sock, &node.rune, id, w, next).await {
                Ok(cur) => {
                    if tx.send((w, cur)).is_err() {
                        return;
                    }
                    next = cur + 1;
                }
                Err(WaitError::Rpc(msg)) => {
                    log!(Warn, "not watching", conn = w.subsystem, err = msg);
                    return;
                }
                Err(WaitError::Conn(msg)) => {
                    log!(Warn, "reconnecting", conn = w.subsystem, reason = msg);
                    break;
                }
            }
        }
    }
}

/// Send one `wait` and read until its reply, answering the node's pings and
/// pinging it in turn so a dead connection doesn't hang here forever.
async fn wait_once(
    sock: &mut LNSocket,
    rune: &str,
    id: u64,
    w: &Watch,
    next: u64,
) -> Result<u64, WaitError> {
    let conn = |e: &dyn std::fmt::Display| WaitError::Conn(e.to_string());
    let params = json!({"subsystem": w.subsystem, "indexname": w.indexname, "nextvalue": next});
    let cmd = CommandoCommand::new(id, "wait".into(), rune.into(), params, None);
    sock.write(&cmd).await.map_err(|e| conn(&e))?;

    let mut reply = Vec::new();
    let mut heard = Instant::now();
    let mut tick = tokio::time::interval(HEARTBEAT);
    tick.tick().await; // the first tick is immediate
    loop {
        tokio::select! {
            // read_custom is cancel-safe, so losing the race to the tick
            // drops no bytes.
            msg = sock.read_custom(|typ, buf| read_incoming_commando_message(typ, buf)) => {
                heard = Instant::now();
                match msg.map_err(|e| conn(&e))? {
                    Message::Ping(ping) => {
                        let pong = Pong { byteslen: ping.ponglen };
                        sock.write(&pong).await.map_err(|e| conn(&e))?;
                    }
                    Message::Custom(IncomingCommandoMessage::Chunk(c)) if c.req_id == id => {
                        reply.extend_from_slice(&c.chunk);
                    }
                    Message::Custom(IncomingCommandoMessage::Done(c)) if c.req_id == id => {
                        reply.extend_from_slice(&c.chunk);
                        let res = parse_reply(&reply)?;
                        return Ok(res[w.indexname].as_u64().unwrap_or(next));
                    }
                    _ => {}
                }
            }
            _ = tick.tick() => {
                if heard.elapsed() > 2 * HEARTBEAT {
                    return Err(WaitError::Conn("no reply to pings".into()));
                }
                let ping = Ping { ponglen: 0, byteslen: 0 };
                sock.write(&ping).await.map_err(|e| conn(&e))?;
            }
        }
    }
}

fn parse_reply(buf: &[u8]) -> Result<Value, WaitError> {
    let mut v: Value = serde_json::from_slice(buf)
        .map_err(|e| WaitError::Conn(format!("bad commando reply: {e}")))?;
    if let Some(err) = v.get("error") {
        return Err(WaitError::Rpc(
            err["message"].as_str().unwrap_or("unknown error").to_string(),
        ));
    }
    Ok(v["result"].take())
}

/// Channel peers by short channel id and by channel id, for naming forwards
/// and closes, which only carry the channel.
#[derive(Default)]
struct Peers {
    by_scid: HashMap<String, String>,
    by_channel: HashMap<String, String>,
    fetched: Option<Instant>,
}

impl Peers {
    fn is_peer(&self, id: &str) -> bool {
        self.by_channel.values().any(|p| p == id)
    }

    fn stale(&self) -> bool {
        self.fetched.is_none_or(|t| t.elapsed() >= PEER_REFRESH)
    }
}

struct Monitor {
    name: String,
    node: Node,
    pubkey: PublicKey,
    replay: u64,
    /// Every call except `wait`. Replaced once its pump stops.
    calls: Arc<CommandoClient>,
    /// Per subsystem, the newest index already reported.
    seen: HashMap<&'static str, u64>,
    /// (payment_hash:groupid) of payments already reported.
    sent: HashSet<String>,
    peers: Peers,
    /// Node id -> alias (None when the node has no announcement).
    aliases: HashMap<String, Option<String>>,
    /// Events printed so far, for the catch-up log lines.
    emitted: u64,
}

impl Monitor {
    async fn start(&mut self) {
        loop {
            match self.call("getinfo", json!({})).await {
                Ok(info) => {
                    log!(Info, "started", node = self.name, alias = str_of(&info["alias"]),
                        version = str_of(&info["version"]));
                    return self.emit(
                        "monitor_started",
                        json!({
                            "id": info["id"],
                            "alias": info["alias"],
                            "version": info["version"],
                            "blockheight": info["blockheight"],
                        }),
                    );
                }
                Err(e) => {
                    log!(Warn, "getinfo failed", err = e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Report everything in `w` past what was already seen, up to at least
    /// `cur`, the index its `wait` just returned.
    async fn catch_up(&mut self, w: &'static Watch, cur: u64) {
        let replay = self.replay;
        if !self.seen.contains_key(w.subsystem) {
            log!(Info, "watching", sub = w.subsystem, index = cur);
        }
        let seen = *self
            .seen
            .entry(w.subsystem)
            .or_insert_with(|| cur.saturating_sub(replay));
        if cur <= seen {
            return;
        }

        // The `wait` task has already moved on, so keep trying until this
        // works rather than skip what changed.
        let params = json!({"index": w.indexname, "start": seen + 1});
        let items = loop {
            match self.call(w.list, params.clone()).await {
                Ok(mut res) => match res[w.field].take() {
                    Value::Array(items) => break items,
                    _ => break Vec::new(),
                },
                Err(e) => {
                    log!(Warn, "list failed", sub = w.subsystem, method = w.list, err = e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        };
        let before = self.emitted;

        for item in &items {
            match w.subsystem {
                "invoices" => self.on_invoice(item),
                "forwards" => self.on_forward(item).await,
                "chainmoves" => self.on_chainmove(item).await,
                "networkevents" => self.on_networkevent(item).await,
                _ => {}
            }
        }
        if w.subsystem == "sendpays" {
            self.on_sendpays(&items).await;
        }

        let index_key = format!("{}_index", w.indexname);
        let newest = items.iter().filter_map(|it| it[&index_key].as_u64()).max();
        let to = newest.unwrap_or(0).max(cur);
        self.seen.insert(w.subsystem, to);

        // Network events include every ping, so they're only logged with -v.
        let level = if w.subsystem == "networkevents" { Debug } else { Info };
        log!(level, "caught_up", sub = w.subsystem, from = seen + 1, to = to,
            items = items.len(), events = self.emitted - before);
    }

    fn on_invoice(&mut self, inv: &Value) {
        if inv["status"] != "paid" {
            return;
        }
        self.emit(
            "invoice_paid",
            json!({
                "label": inv["label"],
                "description": inv["description"],
                "amount_msat": inv["amount_msat"],
                "amount_received_msat": inv["amount_received_msat"],
                "payment_hash": inv["payment_hash"],
                "paid_at": inv["paid_at"],
                "bolt12": inv.get("bolt12").is_some(),
            }),
        );
    }

    async fn on_sendpays(&mut self, items: &[Value]) {
        // A multi-part payment is several sendpays sharing a hash and group.
        let mut groups: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for p in items.iter().filter(|p| p["status"] == "complete") {
            let key = format!("{}:{}", str_of(&p["payment_hash"]), p["groupid"]);
            groups.entry(key).or_default().push(p);
        }
        for (key, parts) in groups {
            if !self.sent.insert(key) {
                continue;
            }
            let first = parts[0];
            let sum = |f: &str| parts.iter().filter_map(|p| p[f].as_u64()).sum::<u64>();
            let amount = sum("amount_msat");
            let sent = sum("amount_sent_msat");
            let alias = self.alias(str_of(&first["destination"])).await;
            self.emit(
                "payment_sent",
                json!({
                    "payment_hash": first["payment_hash"],
                    "destination": first["destination"],
                    "destination_alias": alias,
                    "amount_msat": amount,
                    "amount_sent_msat": sent,
                    "fee_msat": sent.saturating_sub(amount),
                    "parts": parts.len(),
                    "label": first["label"],
                    "description": first["description"],
                    "bolt11": first["bolt11"],
                    "completed_at": first["completed_at"],
                }),
            );
        }
    }

    async fn on_forward(&mut self, f: &Value) {
        if f["status"] != "settled" {
            return;
        }
        let in_peer = self.scid_peer(str_of(&f["in_channel"])).await;
        let out_peer = self.scid_peer(str_of(&f["out_channel"])).await;
        let in_alias = self.alias(&in_peer.unwrap_or_default()).await;
        let out_alias = self.alias(&out_peer.unwrap_or_default()).await;
        self.emit(
            "forward_settled",
            json!({
                "in_channel": f["in_channel"],
                "out_channel": f["out_channel"],
                "in_alias": in_alias,
                "out_alias": out_alias,
                "in_msat": f["in_msat"],
                "out_msat": f["out_msat"],
                "fee_msat": f["fee_msat"],
                "received_time": f["received_time"],
                "resolved_time": f["resolved_time"],
            }),
        );
    }

    async fn on_chainmove(&mut self, m: &Value) {
        let utxo = str_of(&m["utxo"]);
        let txid = utxo.split(':').next();
        match str_of(&m["primary_tag"]) {
            "deposit" if m["account_id"] == "wallet" => self.emit(
                "onchain_deposit",
                json!({
                    "amount_msat": m["credit_msat"],
                    "utxo": utxo,
                    "txid": txid,
                    "blockheight": m["blockheight"],
                }),
            ),
            // Recorded once the funding transaction is buried enough to use.
            "channel_open" => {
                let channel = str_of(&m["account_id"]);
                let peer = str_of(&m["peer_id"]);
                self.peers
                    .by_channel
                    .insert(channel.to_string(), peer.to_string());
                let we_opened = m["extra_tags"]
                    .as_array()
                    .is_some_and(|t| t.iter().any(|t| t == "opener"));
                let alias = self.alias(peer).await;
                self.emit(
                    "channel_opened",
                    json!({
                        "channel_id": channel,
                        "peer_id": peer,
                        "peer_alias": alias,
                        "capacity_msat": m["output_msat"],
                        "to_us_msat": m["credit_msat"],
                        "opener": if we_opened { "local" } else { "remote" },
                        "funding_txid": txid,
                        "blockheight": m["blockheight"],
                    }),
                );
            }
            // Recorded when the funding output is spent, by either side.
            "channel_close" => {
                let channel = str_of(&m["account_id"]);
                let peer = self.channel_peer(channel).await;
                let alias = self.alias(peer.as_deref().unwrap_or("")).await;
                self.emit(
                    "channel_closed",
                    json!({
                        "channel_id": channel,
                        "peer_id": peer,
                        "peer_alias": alias,
                        "capacity_msat": m["output_msat"],
                        "to_us_msat": m["debit_msat"],
                        "funding_txid": txid,
                        "closing_txid": m["spending_txid"],
                        "blockheight": m["blockheight"],
                    }),
                );
            }
            _ => {}
        }
    }

    async fn on_networkevent(&mut self, e: &Value) {
        let typ = match str_of(&e["type"]) {
            "connect" => "peer_connected",
            "disconnect" => "peer_disconnected",
            _ => return,
        };
        // Gossip peers come and go all day; only channel peers are news.
        let peer = str_of(&e["peer_id"]);
        if !self.peers.is_peer(peer) && self.peers.stale() {
            self.fetch_peers().await;
        }
        if !self.peers.is_peer(peer) {
            return;
        }
        let alias = self.alias(peer).await;
        self.emit(
            typ,
            json!({"peer_id": peer, "peer_alias": alias, "reason": e["reason"]}),
        );
    }

    async fn scid_peer(&mut self, scid: &str) -> Option<String> {
        if !self.peers.by_scid.contains_key(scid) && self.peers.stale() {
            self.fetch_peers().await;
        }
        self.peers.by_scid.get(scid).cloned()
    }

    async fn channel_peer(&mut self, channel: &str) -> Option<String> {
        if !self.peers.by_channel.contains_key(channel) {
            self.fetch_peers().await;
        }
        self.peers.by_channel.get(channel).cloned()
    }

    /// Learn which peer each channel is with: open channels from
    /// `listpeerchannels`, plus `listclosedchannels` so a close still names
    /// its peer after CLN has moved the channel there.
    async fn fetch_peers(&mut self) {
        let fields = json!({"peer_id": true, "channel_id": true, "short_channel_id": true});
        for (method, field) in [
            ("listpeerchannels", "channels"),
            ("listclosedchannels", "closedchannels"),
        ] {
            let opts = CallOpts::new().filter(json!({ field: [fields.clone()] }));
            let res = match self.call_with(method, json!({}), opts).await {
                Ok(res) => res,
                Err(e) => {
                    log!(Warn, "peer lookup failed", method = method, err = e);
                    continue;
                }
            };
            for c in res[field].as_array().into_iter().flatten() {
                let (Some(peer), Some(channel)) =
                    (c["peer_id"].as_str(), c["channel_id"].as_str())
                else {
                    continue;
                };
                self.peers
                    .by_channel
                    .insert(channel.to_string(), peer.to_string());
                if let Some(scid) = c["short_channel_id"].as_str() {
                    self.peers
                        .by_scid
                        .insert(scid.to_string(), peer.to_string());
                }
            }
        }
        self.peers.fetched = Some(Instant::now());
    }

    /// The node's alias, looked up once with `listnodes` and then cached.
    async fn alias(&mut self, id: &str) -> Option<String> {
        if id.is_empty() {
            return None;
        }
        if let Some(a) = self.aliases.get(id) {
            return a.clone();
        }
        let res = self.call("listnodes", json!({"id": id})).await.ok()?;
        let alias = res["nodes"][0]["alias"]
            .as_str()
            .filter(|a| !a.is_empty())
            .map(str::to_string);
        self.aliases.insert(id.to_string(), alias.clone());
        alias
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, Error> {
        self.call_with(method, params, CallOpts::new()).await
    }

    async fn call_with(
        &mut self,
        method: &str,
        params: Value,
        opts: CallOpts,
    ) -> Result<Value, Error> {
        let res = tokio::time::timeout(
            CALL_TIMEOUT,
            self.calls.call_with_opts(method, params, opts),
        )
        .await
        .unwrap_or(Err(Error::Io(std::io::ErrorKind::TimedOut)));

        // Calls are retried across reconnects without limit, so BrokenPipe
        // means the pump has given up and stopped, failing everything pending
        // first. Nothing is left on the old client, so start a new one.
        if let Err(Error::Io(std::io::ErrorKind::BrokenPipe)) = res {
            log!(Warn, "reconnecting", conn = "rpc", reason = "pump stopped");
            self.calls = connect_calls(&self.node, self.pubkey).await;
        }
        res
    }

    fn emit(&mut self, typ: &str, fields: Value) {
        self.emitted += 1;
        log!(Info, "event", r#type = typ);
        let mut obj = Map::new();
        obj.insert("type".into(), typ.into());
        obj.insert("node".into(), self.name.as_str().into());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        obj.insert("time".into(), now.into());
        if let Value::Object(fields) = fields {
            obj.extend(fields.into_iter().filter(|(_, v)| !v.is_null()));
        }
        let line = Value::Object(obj).to_string();
        let mut out = std::io::stdout().lock();
        // Whoever was reading has gone away; nothing left to do.
        if writeln!(out, "{line}").and_then(|_| out.flush()).is_err() {
            std::process::exit(0);
        }
    }
}

fn str_of(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

static VERBOSE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, PartialEq)]
enum Level {
    Debug,
    Info,
    Warn,
}
use Level::{Debug, Info, Warn};

/// `log!(Info, "msg", key = value, ...)`: one logfmt line on stderr.
macro_rules! log {
    ($level:expr, $msg:expr $(, $key:ident = $val:expr)* $(,)?) => {
        logfmt($level, $msg, &[$((stringify!($key), &$val as &dyn std::fmt::Display)),*])
    };
}
use log;

fn logfmt(level: Level, msg: &str, fields: &[(&str, &dyn std::fmt::Display)]) {
    if level == Debug && !VERBOSE.load(Ordering::Relaxed) {
        return;
    }
    let level = match level {
        Debug => "debug",
        Info => "info",
        Warn => "warn",
    };
    let mut line = format!("ts={} level={level} src=monitor msg={}", timestamp(), quote(msg));
    for (key, val) in fields {
        let key = key.trim_start_matches("r#");
        line.push_str(&format!(" {key}={}", quote(&val.to_string())));
    }
    eprintln!("{line}");
}

/// A logfmt value: bare when it can be, quoted and escaped otherwise.
fn quote(v: &str) -> String {
    let bare = !v.is_empty()
        && v.chars().all(|c| c > ' ' && c != '"' && c != '=' && c != '\\' && c != '\x7f');
    if bare {
        return v.to_string();
    }
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Now in UTC, eg. 2026-11-12T11:00:38.123Z, without a date crate.
fn timestamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // Howard Hinnant's days_from_civil, inverted.
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        now.subsec_millis()
    )
}
