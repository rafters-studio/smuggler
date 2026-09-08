//! Masterless multicast gossip sync -- the LAN broadcast shape.
//!
//! Spec (`vault/site/homepage.md` -> "LAN broadcast semantics"):
//!
//! - **Masterless / peer-symmetric.** Every node runs the identical loop; there
//!   is no coordinator and no client/server distinction.
//! - **Membership = key possession.** All datagrams are XChaCha20-Poly1305
//!   sealed with a shared key (see [`crate::broadcast::maybe_encrypt`]); holding
//!   the key is the only access control. There is no path-, filename-, or
//!   DB-identity scoping: two replicas of one logical database sync regardless of
//!   where each stores its file. Isolate clusters on one LAN with distinct keys
//!   (a foreign key's datagrams fail AEAD authentication and are dropped).
//!
//!   That mechanism requires a key. A node configured *without* one decrypts
//!   nothing and verifies nothing -- it hands every datagram to the `Msg` parser,
//!   which rejects a foreign cluster's ciphertext as unparseable. The datagram is
//!   still dropped, but by a parse failure rather than an authentication check,
//!   so a keyless node has no cluster isolation in the security sense and no
//!   integrity guarantee on what it does accept: any host on the subnet can
//!   originate rows for it. Run with a key (#313).
//! - **Idempotent apply under a declared policy.** An incoming row is applied
//!   with a single guarded statement, so applying the same row twice is a no-op
//!   and lost datagrams are safe. Which side wins a same-PK collision is
//!   [`BroadcastConfig::conflict_resolution`], which defaults to `remote_wins`
//!   -- the historical last-received-wins behavior, unchanged. `newer_wins`
//!   orders by `max` across
//!   [`BroadcastConfig::ordering_columns`](crate::broadcast::BroadcastConfig::ordering_columns)
//!   and is an explicit opt-in; the comparison rides inside the write
//!   (`ON CONFLICT ... DO UPDATE ... WHERE`), so it is atomic against a
//!   concurrent local write rather than a read-then-write race.
//!
//!   Both peers must opt in. A mesh where one node runs `newer_wins` and
//!   another `remote_wins` converges toward the permissive node -- the policy
//!   is apply-side, so it is not negotiated on the wire and
//!   [`PROTOCOL_VERSION`](crate::broadcast::PROTOCOL_VERSION) is unaffected.
//! - **Heartbeat reconciliation.** Each node periodically multicasts a
//!   `primary_key -> content_hash` [`Digest`](Body::Digest) for every syncable
//!   table. A peer that hears a digest covering rows it lacks (or hashes
//!   differently) multicasts a [`Want`](Body::Want); whoever holds those rows
//!   answers with a [`Delta`](Body::Delta). Late joiners and partition rejoins
//!   converge through the same path with no special case.
//!
//! ## v0.1 boundaries (named, per the smugglr conflict doctrine -- do not hide)
//!
//! - **Concurrent same-PK divergence resolves without a causal record** (no
//!   vector clocks/CRDTs). Under the default `remote_wins` it resolves silently;
//!   under `newer_wins` the loser is counted in
//!   [`GossipEvent::Applied::rejected`] and per-table anomalies are readable via
//!   [`Gossip::ordering_notes`]. Two writes at the identical instant still tie,
//!   and a tie is not accepted -- the two nodes stay divergent and re-exchange
//!   on each heartbeat. UUIDv7 PKs make this rare; the single-writer-per-row
//!   workload makes it rarer. Visible-conflict / strict-replay is v0.2.
//! - **Deletes do not propagate at all** (#311). Not by the heartbeat, and not by
//!   the live [`Delta`](Body::Delta) either: through 0.4.x that packet's `deletes`
//!   list was populated and put on the wire, but no receiver ever applied it, so
//!   the live path documented here as the one that worked never did.
//!   [`Gossip::broadcast_delta`] no longer accepts deletes, and the receive side
//!   drops any a 0.4.x peer still sends. The heartbeat cannot substitute: it
//!   advertises *presence*, so an absent PK is indistinguishable from
//!   not-yet-received -- the digest reconciles upserts/divergence, not deletions.
//!   Tombstone propagation is v0.2; until then, model deletion as a `deleted_at`
//!   column and let it ride the upsert path.
//! - **Schema is the user's.** smugglr syncs rows, never DDL; a row for a table
//!   that does not exist locally is dropped with a warning.

use crate::broadcast::{
    maybe_decrypt, maybe_encrypt, BroadcastConfig, DeltaPacket, ReplayGuard, DEFAULT_PORT,
    PROTOCOL_VERSION, SAFE_PACKET_SIZE,
};
use crate::config::{Config, ConflictResolution};
use crate::datasource::DataSource;
use crate::error::{Result, SyncError};
use crate::local::{LocalDb, UpsertGuard};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Default administratively-scoped IPv4 multicast group for smugglr gossip.
///
/// 239.0.0.0/8 is the IPv4 "Administratively Scoped" block (RFC 2365): routed
/// within an organization, never onto the public internet.
pub const DEFAULT_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 43, 21);

/// Receive buffer: the largest datagram we will accept (one full UDP payload).
///
/// `pub` so callers of [`Gossip::recv_and_handle`] can size the reusable buffer
/// they hoist out of their receive loop (see that method's doc comment).
pub const RECV_BUF: usize = 65_536;

/// Wire headroom reserved when chunking deltas for multicast: the `Msg` JSON
/// wrapper (`{"version":N,"body":{"t":"Delta",...}}`, a fixed ~35 bytes) plus the
/// XChaCha20-Poly1305 nonce+tag (40 bytes). Conservative, so a sealed `Delta`
/// datagram never exceeds [`SAFE_PACKET_SIZE`].
const DELTA_WIRE_RESERVE: usize = 128;

/// Wire headroom reserved when chunking digests for multicast: the
/// XChaCha20-Poly1305 nonce+tag (24 + 16 = 40 bytes) that [`Gossip::seal`] adds
/// on top of the serialized `Msg`. `split_digest` already probes against the
/// full `Msg` envelope, so unlike [`DELTA_WIRE_RESERVE`] only the AEAD seal
/// overhead must be reserved here. Conservative, so a sealed `Digest` datagram
/// never exceeds [`SAFE_PACKET_SIZE`].
const DIGEST_WIRE_RESERVE: usize = 64;

/// A `primary_key -> content_hash` advertisement for one table (one datagram).
///
/// Large tables are chunked across several parts; each part is processed and
/// answered independently (no reassembly), so a lost part only delays the rows
/// it covered until the next heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DigestPacket {
    pub source_id: String,
    pub seq: u64,
    pub part: u16,
    pub total_parts: u16,
    pub table: String,
    /// `primary_key -> content_hash` for this node's rows in `table` (this chunk).
    pub hashes: HashMap<String, String>,
}

/// A request for specific rows the sender is missing or holds a differing hash
/// for. Answered by any node that holds them, over multicast, so every listener
/// converges from a single answer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WantPacket {
    pub source_id: String,
    pub table: String,
    /// Primary keys the sender wants pulled.
    pub pks: Vec<String>,
}

/// The gossip payload carried inside a sealed datagram.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t")]
pub enum Body {
    Digest(DigestPacket),
    Want(WantPacket),
    Delta(DeltaPacket),
}

/// A versioned gossip datagram. Serialized to JSON, then sealed with the shared
/// key before it hits the wire. Membership is key possession: a node that can
/// decrypt the datagram is on the network. There is no path- or filename-based
/// scoping -- two replicas of one logical database sync regardless of where each
/// stores its file. Run isolated clusters on the same LAN by using different keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Msg {
    pub version: u8,
    pub body: Body,
}

impl Msg {
    fn new(body: Body) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| SyncError::Broadcast(format!("msg serialize: {e}")))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| SyncError::Broadcast(format!("msg deserialize: {e}")))
    }
}

/// Why a received datagram was dropped without effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoreReason {
    /// Our own multicast, looped back.
    SelfOrigin,
    /// Sequence already seen (replay guard).
    Replay,
    /// Table not in this node's sync set.
    Table,
    /// Protocol version mismatch.
    Version,
    /// Could not decrypt with our key.
    Undecryptable,
    /// Decrypted bytes did not parse as a `Msg`.
    Malformed,
}

/// Outcome of handling one received datagram. Returned for logging and tests.
#[derive(Debug, Clone, PartialEq)]
pub enum GossipEvent {
    /// A digest part was processed; `wanted` rows were requested via a `Want`.
    Digest { table: String, wanted: usize },
    /// A `Want` was answered by multicasting `rows` rows.
    Served { table: String, rows: usize },
    /// A delta was applied; `rows` rows changed locally and `rejected` rows were
    /// turned away by the configured conflict guard (see
    /// [`BroadcastConfig::conflict_resolution`]). Under the default
    /// `remote_wins` `rejected` is always 0.
    Applied {
        table: String,
        rows: usize,
        rejected: usize,
    },
    /// Dropped without effect; see [`IgnoreReason`].
    Ignored(IgnoreReason),
}

/// Split a `primary_key -> content_hash` map into datagram-sized digest parts
/// (`part`/`total_parts` set, `seq` left 0 for the caller to assign).
///
/// Mirrors [`crate::broadcast::split_delta`]'s sizing and seq convention: entries
/// accumulate until the serialized packet would exceed [`SAFE_PACKET_SIZE`], then
/// a new part starts. The caller stamps a unique seq per part so the replay guard
/// sees one monotonic sequence per source.
pub fn split_digest(
    source_id: &str,
    table: &str,
    hashes: HashMap<String, String>,
) -> Result<Vec<DigestPacket>> {
    let mk = || DigestPacket {
        source_id: source_id.to_string(),
        seq: 0,
        part: 0,
        total_parts: 1,
        table: table.to_string(),
        hashes: HashMap::new(),
    };

    if hashes.is_empty() {
        return Ok(vec![mk()]);
    }

    // Reserve AEAD seal headroom so a *sealed* part stays under the safe MTU,
    // mirroring split_delta's `reserve` discipline (the probe already includes
    // the Msg envelope, so only the 40-byte nonce+tag must be reserved here).
    let limit = SAFE_PACKET_SIZE.saturating_sub(DIGEST_WIRE_RESERVE);

    let mut parts: Vec<DigestPacket> = Vec::new();
    let mut current = mk();

    for (pk, hash) in hashes {
        current.hashes.insert(pk.clone(), hash.clone());
        // Measure with the envelope so we stay under the wire limit end-to-end.
        let probe = Msg::new(Body::Digest(current.clone())).to_bytes()?;
        if probe.len() > limit && current.hashes.len() > 1 {
            current.hashes.remove(&pk);
            parts.push(std::mem::replace(&mut current, mk()));
            current.hashes.insert(pk, hash);
        }
    }
    if !current.hashes.is_empty() {
        parts.push(current);
    }

    let total = parts.len() as u16;
    for (i, p) in parts.iter_mut().enumerate() {
        p.part = i as u16;
        p.total_parts = total;
    }
    Ok(parts)
}

/// The pks a node should pull given a peer's advertised `hashes`: those the peer
/// has that we **lack** or hold a **differing** content hash for.
pub fn want_set(
    peer_hashes: &HashMap<String, String>,
    local_hashes: &HashMap<String, String>,
) -> Vec<String> {
    peer_hashes
        .iter()
        .filter(|(pk, h)| match local_hashes.get(*pk) {
            Some(local) => local != *h, // present but differs -> pull
            None => true,               // absent locally -> pull
        })
        .map(|(pk, _)| pk.clone())
        .collect()
}

/// Bind a UDP socket joined to `group:port` and ready for masterless gossip.
///
/// `SO_REUSEADDR` + `SO_REUSEPORT` let multiple nodes share the port on one host
/// (needed for tests and co-located instances). `multicast_loop` is on so
/// same-host peers hear each other; self-origin datagrams are filtered by
/// `source_id`/`instance_id` in [`Gossip::handle`].
fn bind_multicast(port: u16, group: Ipv4Addr) -> Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    fn io(what: &'static str) -> impl Fn(std::io::Error) -> SyncError {
        move |e| SyncError::Broadcast(format!("{what}: {e}"))
    }

    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(io("socket"))?;
    sock.set_reuse_address(true).map_err(io("SO_REUSEADDR"))?;
    #[cfg(unix)]
    sock.set_reuse_port(true).map_err(io("SO_REUSEPORT"))?;
    sock.set_nonblocking(true).map_err(io("nonblocking"))?;

    let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    sock.bind(&bind.into()).map_err(io("bind"))?;
    sock.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)
        .map_err(io("join_multicast_v4"))?;
    sock.set_multicast_loop_v4(true).map_err(io("loop_v4"))?;

    let std_sock: std::net::UdpSocket = sock.into();
    UdpSocket::from_std(std_sock).map_err(io("from_std"))
}

/// A masterless gossip node: one multicast socket plus the identical
/// broadcast/listen loop every peer runs.
pub struct Gossip {
    socket: Arc<UdpSocket>,
    dest: SocketAddrV4,
    instance_id: String,
    key: Option<[u8; 32]>,
    seq: Arc<AtomicU64>,
    replay: Arc<Mutex<ReplayGuard>>,
    /// Same-PK resolution policy, taken from the `BroadcastConfig` this node
    /// bound with -- alongside port, identity, and key, which are read from the
    /// same place.
    conflict_resolution: ConflictResolution,
    /// Configured ordering columns for `newer_wins` (empty = fall back to
    /// `[sync].timestamp_column` at apply time).
    ordering_columns: Vec<String>,
    /// Per-table apply diagnostics, recorded once and readable via
    /// [`Gossip::ordering_notes`].
    notes: Arc<Mutex<HashMap<String, OrderingNote>>>,
}

/// A once-per-table fact about how `newer_wins` is actually behaving on a table.
///
/// Logged once and kept queryable, because both conditions it records are
/// operator-actionable and neither is visible from the outside: a mesh that has
/// silently stopped ordering, or two nodes that disagree about how a timestamp
/// is represented. A per-packet log would be noise; silence would be worse --
/// the failure this whole issue exists to remove is a user who believes they are
/// ordered and is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderingNote {
    /// The table this note is about.
    pub table: String,
    /// `newer_wins` was configured but the table has none of the ordering
    /// columns, so apply fell back to blind overwrite for this table.
    pub ordering_unavailable: bool,
    /// Ordering columns whose incoming SQLite storage class does not match the
    /// class already stored locally -- an integer Unix time on one node against
    /// ISO-8601 text on the other, say.
    ///
    /// The comparison still terminates (SQLite's ordering across storage classes
    /// is total, so exactly one side of any pair accepts and the mesh
    /// quiesces), but the winner is arbitrary rather than chronological. Two
    /// nodes disagreeing on a representation is a fact an operator needs, not
    /// merely a convergence condition.
    pub representation_mismatch: Vec<String>,
}

impl Gossip {
    /// Join the multicast group and prepare to gossip. Membership is the shared
    /// key in `broadcast` -- any node that can decrypt is on the network.
    pub async fn bind(broadcast: &BroadcastConfig, group: Ipv4Addr) -> Result<Self> {
        let port = if broadcast.port == 0 {
            DEFAULT_PORT
        } else {
            broadcast.port
        };
        let socket = bind_multicast(port, group)?;
        let key = broadcast.encryption_key()?;
        if key.is_none() {
            // #313 review: removing the first-byte sniff also removed the only
            // runtime signal a keyless node ever emitted ("Dropping encrypted
            // packet: no secret configured"). That line was per-packet and fired
            // at ~1/128, but it was the one thing that told an operator who MEANT
            // to set a key that they had not -- a typo'd field name or an
            // unexpanded env var otherwise produces silence and a mesh that
            // simply never converges with its keyed peers.
            //
            // Emitted once at bind rather than per datagram: it does not depend
            // on a foreign packet happening to arrive, and it cannot become the
            // log spam a per-packet warning would be now that every datagram
            // reaches this path.
            warn!(
                "multicast bound on port {port} with NO cluster secret: datagrams are \
                 unauthenticated and unencrypted, this node has no cluster isolation, and any \
                 host on the subnet can originate rows for it. Peers running with a key cannot \
                 sync with this node. Set [broadcast].secret if this was not intended."
            );
        }
        Ok(Self {
            socket: Arc::new(socket),
            dest: SocketAddrV4::new(group, port),
            instance_id: broadcast.resolve_instance_id(),
            key,
            seq: Arc::new(AtomicU64::new(0)),
            replay: Arc::new(Mutex::new(ReplayGuard::new())),
            conflict_resolution: broadcast.conflict_resolution,
            ordering_columns: broadcast.ordering_columns.clone(),
            notes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// The per-table apply diagnostics recorded so far -- see [`OrderingNote`].
    ///
    /// Empty under the default `remote_wins`; an entry appears only when
    /// `newer_wins` is configured and the table cannot honor it as intended.
    pub async fn ordering_notes(&self) -> Vec<OrderingNote> {
        let notes = self.notes.lock().await;
        let mut out: Vec<OrderingNote> = notes.values().cloned().collect();
        out.sort_by(|a, b| a.table.cmp(&b.table));
        out
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// This node's `primary_key -> content_hash` map for `table`.
    async fn row_hashes(
        &self,
        local: &LocalDb,
        config: &Config,
        table: &str,
    ) -> Result<HashMap<String, String>> {
        // The digest advertises content hashes to peers, so it must cover the
        // SAME column set the diff path hashes -- `hash_excluded_columns`, not
        // `exclude_columns` alone. A node whose digest hashed converge columns
        // while its peer's diff did not would advertise a hash no peer can ever
        // match, and the row would read as divergent on every heartbeat forever
        // (#293; the #292 blob-encoding failure with a different cause).
        Ok(local
            .get_row_metadata(
                table,
                &config.sync.timestamp_column,
                &config.sync.hash_excluded_columns(),
            )
            .await?
            .into_iter()
            .map(|(pk, m)| (pk, m.content_hash))
            .collect())
    }

    /// Seal a message body into an on-the-wire datagram (JSON + AEAD).
    fn seal(&self, body: Body) -> Result<Vec<u8>> {
        let plain = Msg::new(body).to_bytes()?;
        maybe_encrypt(&plain, &self.key)
    }

    /// Seal and multicast a batch of bodies to the group.
    async fn emit(&self, bodies: Vec<Body>) -> Result<()> {
        for body in bodies {
            let sealed = self.seal(body)?;
            self.socket
                .send_to(&sealed, self.dest)
                .await
                .map_err(|e| SyncError::Broadcast(format!("multicast send: {e}")))?;
        }
        Ok(())
    }

    /// Build the digest datagrams for one heartbeat: a `primary_key ->
    /// content_hash` advertisement, chunked, for every syncable table.
    pub async fn digest_bodies(&self, local: &LocalDb, config: &Config) -> Result<Vec<Body>> {
        let tables: Vec<String> = local
            .list_tables()
            .await?
            .into_iter()
            .filter(|t| config.should_sync_table(t))
            .collect();

        let mut bodies = Vec::new();
        for table in &tables {
            let hashes = self.row_hashes(local, config, table).await?;
            for mut part in split_digest(&self.instance_id, table, hashes)? {
                part.seq = self.next_seq();
                bodies.push(Body::Digest(part));
            }
        }
        Ok(bodies)
    }

    /// Multicast a content-hash digest for every syncable table -- one heartbeat.
    /// Returns the number of digest datagrams sent.
    pub async fn broadcast_digests(&self, local: &LocalDb, config: &Config) -> Result<usize> {
        let bodies = self.digest_bodies(local, config).await?;
        let n = bodies.len();
        self.emit(bodies).await?;
        Ok(n)
    }

    /// Build delta datagrams for `rows` of `table` (a live write or a `Want`
    /// answer), each part given a unique seq so the replay guard dedupes.
    ///
    /// Always sends an empty `deletes` list. The wire field exists and
    /// [`crate::broadcast::split_delta`] still packs it, but no receiver applies
    /// deletes (see [`Gossip::broadcast_delta`]), so populating it here would put
    /// bytes on the wire that every peer ignores.
    fn delta_bodies(
        &self,
        table: &str,
        upserts: Vec<HashMap<String, serde_json::Value>>,
    ) -> Result<Vec<Body>> {
        let parts = crate::broadcast::split_delta(
            &self.instance_id,
            0,
            table,
            upserts,
            Vec::new(),
            DELTA_WIRE_RESERVE,
        )?;
        Ok(parts
            .into_iter()
            .map(|mut part| {
                part.seq = self.next_seq();
                Body::Delta(part)
            })
            .collect())
    }

    /// Multicast `rows` of `table` as a delta. Returns rows sent.
    ///
    /// # Deletes are not propagated (#311)
    ///
    /// This takes upserts only. Through 0.4.x it also took a `deletes: Vec<String>`
    /// list, which was packed onto the wire and then applied by nothing: the
    /// receive path logs the count and drops it (see `on_delta`). A parameter that
    /// silently discards the caller's deletions is worse than an absent one, so it
    /// is gone rather than merely documented.
    ///
    /// Deleting a row therefore does not replicate by any path today. The
    /// heartbeat cannot cover the gap either: a digest advertises *presence*, so an
    /// absent primary key is indistinguishable from one not yet received. Real
    /// delete propagation needs tombstones and is v0.2.
    ///
    /// This is a hard boundary, not an oversight to route around. Applying a bare
    /// delete on receipt would be *less* correct than dropping it: with no
    /// tombstone, any peer that still holds the row re-gossips it on the next
    /// heartbeat and resurrects it.
    ///
    /// Note the resurrection needs no help from the conflict policy, which an
    /// earlier version of this comment got wrong by blaming `newer_wins`. Once
    /// the row is physically gone locally, the peer's copy arrives with NO
    /// primary-key collision to guard -- it is a plain insert, and every policy
    /// admits it. `remote_wins`, `local_wins` and `newer_wins` all resurrect it,
    /// because a conflict guard only fires when there is a local row to conflict
    /// with. That makes the case against applying bare deletes stronger than the
    /// policy-specific one, not weaker. Model deletion as a soft-delete column
    /// (a `deleted_at` upsert) if you need it now; that rides the upsert path,
    /// converges, and is what the `ordering_columns` list is a `max` over so a
    /// tombstone stamping only `deleted_at` is not a tie that loses.
    ///
    /// The wire format is unchanged: `DeltaPacket` keeps its `deletes` field, so
    /// 0.4.x peers still parse our datagrams and we still parse theirs (a peer
    /// that sends deletes has them dropped, exactly as before).
    pub async fn broadcast_delta(
        &self,
        table: &str,
        upserts: Vec<HashMap<String, serde_json::Value>>,
    ) -> Result<usize> {
        let rows = upserts.len();
        let bodies = self.delta_bodies(table, upserts)?;
        self.emit(bodies).await?;
        Ok(rows)
    }

    /// Receive one datagram, act on it, and multicast any response.
    ///
    /// This is the whole peer-symmetric loop body: a `Digest` we hear may make us
    /// send a `Want`; a `Want` we hear may make us send a `Delta`; a `Delta` we
    /// hear we apply idempotently.
    ///
    /// `buf` is a scratch receive buffer owned by the caller, at least
    /// [`RECV_BUF`] bytes -- callers running a receive loop should allocate it
    /// once outside the loop and pass the same buffer on every call, so a
    /// steady-state gossip listener does not allocate+zero 64 KiB per datagram.
    pub async fn recv_and_handle(
        &self,
        buf: &mut [u8],
        local: &LocalDb,
        config: &Config,
    ) -> Result<GossipEvent> {
        let (n, _addr) = self
            .socket
            .recv_from(buf)
            .await
            .map_err(|e| SyncError::Broadcast(format!("multicast recv: {e}")))?;
        let (event, out) = self.handle(&buf[..n], local, config).await?;
        self.emit(out).await?;
        Ok(event)
    }

    /// Decode, validate, and decide on one raw datagram. Returns the local effect
    /// plus any datagrams to multicast in response. Pure of socket I/O so the
    /// full protocol is testable deterministically (see tests).
    pub async fn handle(
        &self,
        datagram: &[u8],
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        let none = Vec::new();
        // A datagram we cannot turn into plaintext is not ours: a foreign key
        // (AEAD auth failure), a runt packet, or noise. Drop it as a clean,
        // counted Ignored -- never an error -- so a different-keyed cluster
        // sharing the LAN is silent, not a stream of recv-error logs.
        let plain = match maybe_decrypt(datagram, &self.key) {
            Ok(Some(p)) => p,
            Ok(None) | Err(_) => {
                return Ok((GossipEvent::Ignored(IgnoreReason::Undecryptable), none))
            }
        };
        let msg = match Msg::from_bytes(&plain) {
            Ok(m) => m,
            Err(_) => return Ok((GossipEvent::Ignored(IgnoreReason::Malformed), none)),
        };
        if msg.version != PROTOCOL_VERSION {
            return Ok((GossipEvent::Ignored(IgnoreReason::Version), none));
        }

        match msg.body {
            Body::Digest(d) => self.on_digest(d, local, config).await,
            Body::Want(w) => self.on_want(w, local, config).await,
            Body::Delta(d) => self.on_delta(d, local, config).await,
        }
    }

    async fn on_digest(
        &self,
        d: DigestPacket,
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        if d.source_id == self.instance_id {
            return Ok((GossipEvent::Ignored(IgnoreReason::SelfOrigin), Vec::new()));
        }
        if !self.replay.lock().await.check(&d.source_id, d.seq) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Replay), Vec::new()));
        }
        if !config.should_sync_table(&d.table) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Table), Vec::new()));
        }

        let local_hashes = match self.row_hashes(local, config, &d.table).await {
            Ok(h) => h,
            // The ONLY benign cause, and the reason this arm exists: the table
            // is absent here and present on the peer. A late joiner, or one
            // that has not created it yet. An empty local view is the correct
            // reading -- want everything the peer advertises and converge.
            // `missing_table_still_wants_everything_on_digest` pins it (#269).
            Err(SyncError::TableNotFound(_)) => {
                debug!(
                    "no local table '{}'; treating as empty so we want everything",
                    d.table
                );
                HashMap::new()
            }
            // Everything else propagates, and there is deliberately no
            // catch-all (#332). An error that is not "the table is not here"
            // is a refusal or a fault, and neither one means "I have no rows".
            // Reading it as an empty view Wants every row the peer advertises
            // and applies them through `on_delta`, which bypasses the
            // duplicate-`__pk` guard entirely (#269) and is not itself guarded
            // until #278 -- so a misconfiguration becomes a full-table pull
            // down the one unguarded path, on the transport where cross-node
            // key collisions actually originate.
            //
            // What reaches here today, from `table_info_inner` and
            // `get_row_metadata_inner`:
            //   - NoPrimaryKey: the table exists and cannot be synced at all.
            //     smugglr's identity IS the primary key. Full-pulling forever
            //     while every status reads fine is the defect #332 names.
            //   - DuplicatePrimaryKey: a refusal under `DuplicatePkPolicy`.
            //   - a SQLite failure from the pragma or the scan: a fault, and a
            //     node that cannot read its own rows must not conclude it has
            //     none.
            //
            // Listed rather than matched individually so the compiler does not
            // let a fourth cause inherit an arm chosen for the first three --
            // but a new cause added to `table_info_inner` now refuses by
            // DEFAULT and has to be argued into the benign arm above, which is
            // the direction that fails safe.
            //
            // `recv_and_handle`'s caller logs this at `warn`, matching the
            // emission side (`digest_bodies` propagates with `?`), so a node
            // stops pulling rather than silently stopping advertising while
            // still pulling.
            Err(e) => return Err(e),
        };

        let want = want_set(&d.hashes, &local_hashes);
        let wanted = want.len();
        let out = if want.is_empty() {
            Vec::new()
        } else {
            vec![Body::Want(WantPacket {
                source_id: self.instance_id.clone(),
                table: d.table.clone(),
                pks: want,
            })]
        };
        Ok((
            GossipEvent::Digest {
                table: d.table,
                wanted,
            },
            out,
        ))
    }

    async fn on_want(
        &self,
        w: WantPacket,
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        if w.source_id == self.instance_id {
            return Ok((GossipEvent::Ignored(IgnoreReason::SelfOrigin), Vec::new()));
        }
        if !config.should_sync_table(&w.table) || w.pks.is_empty() {
            return Ok((GossipEvent::Ignored(IgnoreReason::Table), Vec::new()));
        }
        let rows = match local.get_rows(&w.table, &w.pks).await {
            Ok(r) => r,
            // The peer wants a table this node does not have. Serving nothing
            // is the correct answer and `Served { rows: 0 }` states it
            // truthfully. The schema is the user's (#415).
            Err(SyncError::TableNotFound(_)) => {
                debug!("cannot serve absent table '{}'; serving nothing", w.table);
                return Ok((
                    GossipEvent::Served {
                        table: w.table,
                        rows: 0,
                    },
                    Vec::new(),
                ));
            }
            // Anything else is a refusal or a fault, and reporting it as
            // `Served { rows: 0 }` makes it indistinguishable from "the peer
            // legitimately needed nothing" -- the same conflation #332 fixed in
            // `on_digest`. `NoPrimaryKey` and a SQLite fault both reach here
            // through `get_rows_inner`'s `table_info_inner` call.
            Err(e) => return Err(e),
        };
        if rows.is_empty() {
            return Ok((
                GossipEvent::Served {
                    table: w.table,
                    rows: 0,
                },
                Vec::new(),
            ));
        }
        let n = rows.len();
        let out = self.delta_bodies(&w.table, rows)?;
        Ok((
            GossipEvent::Served {
                table: w.table,
                rows: n,
            },
            out,
        ))
    }

    async fn on_delta(
        &self,
        d: DeltaPacket,
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        if d.source_id == self.instance_id {
            return Ok((GossipEvent::Ignored(IgnoreReason::SelfOrigin), Vec::new()));
        }
        if !self.replay.lock().await.check(&d.source_id, d.seq) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Replay), Vec::new()));
        }
        if !config.should_sync_table(&d.table) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Table), Vec::new()));
        }

        let mut changed = 0;
        let mut rejected = 0;
        if !d.upserts.is_empty() {
            // Only newer_wins needs an ordering signal; the other policies are
            // decided by the shape of the write alone.
            let ordering: Vec<String> = match self.conflict_resolution {
                ConflictResolution::NewerWins if self.ordering_columns.is_empty() => {
                    vec![config.sync.timestamp_column.clone()]
                }
                ConflictResolution::NewerWins => self.ordering_columns.clone(),
                _ => Vec::new(),
            };
            let guard = match self.conflict_resolution {
                ConflictResolution::RemoteWins => UpsertGuard::Replace,
                ConflictResolution::LocalWins => UpsertGuard::KeepLocal,
                ConflictResolution::NewerWins => UpsertGuard::NewerBy(&ordering),
            };

            match local.upsert_rows_guarded(&d.table, &d.upserts, guard) {
                Ok(outcome) => {
                    changed += outcome.applied;
                    rejected += outcome.rejected;
                    if !ordering.is_empty() {
                        self.record_ordering(&d.table, &d.upserts, local, &ordering, &outcome)
                            .await;
                    }
                }
                // Schema is the user's; a row for a table this node does not
                // have is dropped. That is the ONLY benign cause, and the
                // comment here used to name it while the arm caught everything
                // (#415).
                Err(SyncError::TableNotFound(_)) => {
                    debug!("drop delta for absent table '{}'", d.table)
                }
                // Everything else surfaces. Reported as `Applied { rows: 0,
                // rejected: 0 }` it is byte-identical to "there was nothing to
                // apply", so a node whose table cannot take writes at all --
                // `NoPrimaryKey` reaches here through
                // `upsert_rows_guarded_inner`'s `table_info_inner` call --
                // silently drops every live delta for that table forever, with
                // a log line as the only trace. That is worse than #332's
                // instance rather than milder: #332 pulled rows it should not
                // have and this one stops converging while every event says it
                // is fine.
                Err(e) => return Err(e),
            }
        }
        // Deletes are parsed and dropped, never applied (#311). We no longer send
        // them, but a 0.4.x peer still can, so the field is read and discarded
        // rather than treated as malformed. Applying it would need a tombstone --
        // without one, any peer still holding the row resurrects it on the next
        // heartbeat. See `broadcast_delta` for why this is a boundary, not a gap.
        if !d.deletes.is_empty() {
            debug!(
                "dropping {} delete(s) for '{}': delete propagation needs tombstones (v0.2, #311)",
                d.deletes.len(),
                d.table
            );
        }
        Ok((
            GossipEvent::Applied {
                table: d.table,
                rows: changed,
                rejected,
            },
            Vec::new(),
        ))
    }

    /// Record the once-per-table apply diagnostics for `table`, logging each
    /// exactly once. Cheap after the first delta for a table: the lock is taken,
    /// the entry is found, and nothing else runs.
    ///
    /// The lock is released across the probe rather than held, so two deltas for
    /// the same table racing on their very first packet can both probe and both
    /// log. That is deliberate: holding an async mutex across blocking SQLite
    /// calls would serialize the apply path to buy a duplicate log line, and the
    /// recorded note is identical either way.
    async fn record_ordering(
        &self,
        table: &str,
        upserts: &[HashMap<String, serde_json::Value>],
        local: &LocalDb,
        ordering: &[String],
        outcome: &crate::local::UpsertOutcome,
    ) {
        if self.notes.lock().await.contains_key(table) {
            return;
        }

        if outcome.ordering_unavailable {
            warn!(
                "table '{}' has none of the configured ordering columns {:?}; \
                 newer_wins cannot order it and rows are applied blind",
                table, ordering
            );
        }

        // Compare the storage class the peer sent against the class already
        // stored, per ordering column. One bounded probe per column, once per
        // table, and only under an ordering policy.
        let mut mismatch = Vec::new();
        for col in ordering {
            let Some(remote_class) = upserts
                .iter()
                .filter_map(|r| r.get(col))
                .find_map(json_storage_class)
            else {
                continue;
            };
            match local.stored_storage_class(table, col) {
                Ok(Some(local_class)) if local_class != remote_class => {
                    warn!(
                        "table '{}' column '{}': peer sends {} but {} is stored locally -- \
                         newer_wins still converges, but the winner is arbitrary rather than \
                         chronological",
                        table, col, remote_class, local_class
                    );
                    mismatch.push(col.clone());
                }
                Ok(_) => {}
                // Swallowed on purpose, and NOT the #332/#415 pattern even
                // though it looks like it. Written down because a reviewer
                // checked and the next one should not have to.
                //
                // This runs only inside `on_delta`'s `Ok` arm, so the write has
                // already succeeded and the table is known to exist with a
                // usable key -- `TableNotFound` and `NoPrimaryKey` cannot reach
                // here. `stored_storage_class` does not call `table_info`, so
                // what is left is a raw SQLite error, and in practice one
                // error: `ordering` comes from config and is never filtered
                // against the table's real columns, so a configured column the
                // table does not have fails the probe.
                //
                // That is expected rather than exceptional, and the probe is a
                // diagnostic: it decides whether an `OrderingNote` mismatch
                // warning is emitted, never whether a row is applied. Surfacing
                // it would turn a config typo into a refused delta.
                Err(e) => debug!("cannot probe '{}'.'{}': {}", table, col, e),
            }
        }

        self.notes.lock().await.insert(
            table.to_string(),
            OrderingNote {
                table: table.to_string(),
                ordering_unavailable: outcome.ordering_unavailable,
                representation_mismatch: mismatch,
            },
        );
    }
}

/// The SQLite storage class a JSON value binds as, matching `JsonToSql`. `None`
/// for JSON null, which carries no class to compare.
fn json_storage_class(v: &serde_json::Value) -> Option<&'static str> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(_) => Some("integer"),
        serde_json::Value::Number(n) if n.as_i64().is_some() => Some("integer"),
        serde_json::Value::Number(n) if n.as_f64().is_some() => Some("real"),
        _ => Some("text"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(db_path: &str) -> Config {
        Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some(db_path.to_string()),
            sync: crate::config::SyncConfig::default(),
            stash: None,
            target: Some(crate::config::TargetConfig::Sqlite {
                database: "unused".to_string(),
            }),
            broadcast: None,
        }
    }

    fn seed(path: &str, rows: &[(&str, &str)]) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);")
            .unwrap();
        for (id, name) in rows {
            conn.execute(
                "INSERT INTO users VALUES (?1, ?2, '2026-01-01T00:00:00Z')",
                [id, name],
            )
            .unwrap();
        }
    }

    fn count(path: &str) -> i64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .unwrap()
    }

    fn name_of(path: &str, id: &str) -> Option<String> {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row("SELECT name FROM users WHERE id = ?1", [id], |r| r.get(0))
            .ok()
    }

    #[test]
    fn msg_tag_roundtrip() {
        for body in [
            Body::Want(WantPacket {
                source_id: "a".into(),
                table: "users".into(),
                pks: vec!["1".into()],
            }),
            Body::Digest(DigestPacket {
                source_id: "a".into(),
                seq: 7,
                part: 0,
                total_parts: 1,
                table: "users".into(),
                hashes: HashMap::from([("1".into(), "h".into())]),
            }),
        ] {
            let msg = Msg::new(body);
            let bytes = msg.to_bytes().unwrap();
            assert_eq!(Msg::from_bytes(&bytes).unwrap(), msg);
        }
    }

    #[test]
    fn want_set_lacks_and_differs() {
        let peer = HashMap::from([
            ("1".to_string(), "h1".to_string()),
            ("2".to_string(), "h2".to_string()),
            ("3".to_string(), "h3".to_string()),
        ]);
        let local = HashMap::from([
            ("1".to_string(), "h1".to_string()), // identical -> skip
            ("2".to_string(), "DIFFERENT".to_string()), // differ -> want
                                                 // 3 missing -> want
        ]);
        let mut got = want_set(&peer, &local);
        got.sort();
        assert_eq!(got, vec!["2".to_string(), "3".to_string()]);
    }

    #[test]
    fn split_digest_chunks_large_maps() {
        let big: HashMap<String, String> = (0..2000)
            .map(|i| (format!("pk-{i:08}"), format!("hash-{i:016}")))
            .collect();
        let parts = split_digest("node", "users", big.clone()).unwrap();
        assert!(parts.len() > 1, "2000 entries must span multiple datagrams");
        // Each part fits the wire, parts are numbered 0..total, union is lossless.
        let total = parts.len() as u16;
        let mut nums = std::collections::HashSet::new();
        let mut reunion = HashMap::new();
        for p in &parts {
            let sealed = Msg::new(Body::Digest(p.clone())).to_bytes().unwrap();
            assert!(sealed.len() <= SAFE_PACKET_SIZE, "part exceeds safe size");
            assert_eq!(p.total_parts, total, "total_parts set on every part");
            assert!(nums.insert(p.part), "part numbers must be unique");
            reunion.extend(p.hashes.clone());
        }
        assert_eq!(reunion, big, "chunking must be lossless");
    }

    /// Regression for #168/#173: a sealed digest part must stay under the safe
    /// MTU. Before the fix, split_digest sized parts against the bare `Msg`
    /// envelope up to SAFE_PACKET_SIZE, so adding the 40-byte AEAD seal pushed
    /// the on-the-wire datagram over the limit. Mirrors
    /// `keyed_delta_parts_stay_under_mtu_when_sealed` by sealing each part.
    #[tokio::test]
    async fn keyed_digest_parts_stay_under_mtu_when_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        seed(&a_path, &[]);
        seed(&b_path, &[]);
        let (a, _ac, _al, _b, _bc, _bl) =
            two_nodes_keyed(&a_path, &b_path, Some(key), Some(key)).await;

        let big: HashMap<String, String> = (0..2000)
            .map(|i| (format!("pk-{i:020}"), format!("hash-{i:032}")))
            .collect();
        let parts = split_digest("node-a", "users", big).unwrap();
        assert!(parts.len() > 1, "2000 entries must span multiple datagrams");
        for part in parts {
            let sealed = a.seal(Body::Digest(part)).unwrap();
            assert!(
                sealed.len() <= SAFE_PACKET_SIZE,
                "sealed digest part is {} bytes, exceeds SAFE_PACKET_SIZE {}",
                sealed.len(),
                SAFE_PACKET_SIZE
            );
        }
    }

    // -- Ordering-aware apply (#310) --

    /// Seed a legion-shaped table: the ordering key is the max over three
    /// columns and `deleted_at` is NULL on every live row.
    fn seed_ordered(path: &str, rows: &[(&str, &str, &str, Option<&str>)]) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 body TEXT,
                 created_at TEXT,
                 updated_at TEXT,
                 deleted_at TEXT
             );",
        )
        .unwrap();
        for (id, body, updated, deleted) in rows {
            conn.execute(
                "INSERT INTO notes VALUES (?1, ?2, '2026-01-01T00:00:00+00:00', ?3, ?4)",
                rusqlite::params![id, body, updated, deleted],
            )
            .unwrap();
        }
    }

    fn body_of(path: &str, id: &str) -> Option<String> {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row("SELECT body FROM notes WHERE id = ?1", [id], |r| r.get(0))
            .ok()
    }

    fn legion_policy(bc: &mut BroadcastConfig) {
        bc.conflict_resolution = ConflictResolution::NewerWins;
        bc.ordering_columns = vec![
            "created_at".to_string(),
            "updated_at".to_string(),
            "deleted_at".to_string(),
        ];
    }

    /// The trap this issue exists to avoid walking into. An existing multicast
    /// deployment carries no `[broadcast].conflict_resolution`, and
    /// `[sync].conflict_resolution` defaults to LocalWins. If the apply path
    /// read the sync-scoped field, every such deployment would silently stop
    /// accepting peer rows -- a convergence break shipped as a bugfix.
    #[tokio::test]
    async fn default_policy_is_remote_wins_not_the_sync_scoped_default() {
        assert_eq!(
            BroadcastConfig::default().conflict_resolution,
            ConflictResolution::RemoteWins,
        );
        assert_eq!(
            crate::config::SyncConfig::default().conflict_resolution,
            ConflictResolution::LocalWins,
            "the sync-scoped default is the one we must NOT inherit"
        );

        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        // B's row is NEWER. Under the historical behavior A still takes it.
        seed_ordered(
            &a_path,
            &[("1", "a-newer", "2026-09-01T00:00:00+00:00", None)],
        );
        seed_ordered(
            &b_path,
            &[("1", "b-older", "2026-02-01T00:00:00+00:00", None)],
        );
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        let digest = only(b.digest_bodies(&b_local, &b_cfg).await.unwrap());
        let (_ev, out) = route(&b, digest, &a, &a_local, &a_cfg).await;
        let (_ev, out) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        let (ev, _) = route(&b, only(out), &a, &a_local, &a_cfg).await;

        assert!(
            matches!(
                ev,
                GossipEvent::Applied {
                    rows: 1,
                    rejected: 0,
                    ..
                }
            ),
            "the default must stay last-received-wins, got {ev:?}"
        );
        assert_eq!(body_of(&a_path, "1").as_deref(), Some("b-older"));
    }

    /// The fix, end to end over the gossip path: with `newer_wins` opted in, a
    /// peer's stale row does not overwrite a newer local row -- and the loss is
    /// counted rather than silent.
    #[tokio::test]
    async fn newer_wins_turns_away_a_stale_peer_row() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed_ordered(
            &a_path,
            &[("1", "a-newer", "2026-09-01T00:00:00+00:00", None)],
        );
        seed_ordered(
            &b_path,
            &[("1", "b-older", "2026-02-01T00:00:00+00:00", None)],
        );
        let (a, a_cfg, a_local, b, b_cfg, b_local) =
            two_nodes_with(&a_path, &b_path, None, None, legion_policy).await;

        // B advertises; A wants (hashes differ); B serves; A applies -- and
        // rejects, because B's row is older.
        let digest = only(b.digest_bodies(&b_local, &b_cfg).await.unwrap());
        let (_ev, out) = route(&b, digest, &a, &a_local, &a_cfg).await;
        let (_ev, out) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        let (ev, _) = route(&b, only(out), &a, &a_local, &a_cfg).await;

        assert!(
            matches!(
                ev,
                GossipEvent::Applied {
                    rows: 0,
                    rejected: 1,
                    ..
                }
            ),
            "got {ev:?}"
        );
        assert_eq!(
            body_of(&a_path, "1").as_deref(),
            Some("a-newer"),
            "the newer local row must survive"
        );

        // And the exchange terminates: the other direction accepts, so the mesh
        // converges on A's row rather than re-firing forever.
        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (_ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        let (_ev, out) = route(&b, only(out), &a, &a_local, &a_cfg).await;
        let (ev, _) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        assert!(
            matches!(
                ev,
                GossipEvent::Applied {
                    rows: 1,
                    rejected: 0,
                    ..
                }
            ),
            "got {ev:?}"
        );
        assert_eq!(body_of(&b_path, "1").as_deref(), Some("a-newer"));
    }

    /// legion's actual case: a tombstone stamps `deleted_at` and leaves
    /// `updated_at` alone. It must still win, which is only true because the
    /// ordering signal is a column LIST evaluated with max.
    #[tokio::test]
    async fn newer_wins_propagates_a_deleted_at_only_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        let stamp = "2026-05-01T00:00:00+00:00";
        seed_ordered(&a_path, &[("1", "live", stamp, None)]);
        seed_ordered(
            &b_path,
            &[("1", "tombstoned", stamp, Some("2026-06-01T00:00:00+00:00"))],
        );
        let (a, a_cfg, a_local, b, b_cfg, b_local) =
            two_nodes_with(&a_path, &b_path, None, None, legion_policy).await;

        let digest = only(b.digest_bodies(&b_local, &b_cfg).await.unwrap());
        let (ev, out) = route(&b, digest, &a, &a_local, &a_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 1, .. }),
            "a deleted_at-only write must be visible in the digest, got {ev:?}"
        );
        let (_ev, out) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        let (ev, _) = route(&b, only(out), &a, &a_local, &a_cfg).await;

        assert!(
            matches!(
                ev,
                GossipEvent::Applied {
                    rows: 1,
                    rejected: 0,
                    ..
                }
            ),
            "the tombstone must win on max(deleted_at), got {ev:?}"
        );
        assert_eq!(body_of(&a_path, "1").as_deref(), Some("tombstoned"));
    }

    /// Two nodes disagreeing about how a timestamp is represented is an
    /// operator-actionable fact, not merely a convergence condition. SQLite's
    /// cross-class ordering is total, so the exchange still terminates -- but
    /// the winner is arbitrary rather than chronological, and that must be
    /// visible.
    #[tokio::test]
    async fn representation_mismatch_is_recorded_once_per_table() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed_ordered(
            &a_path,
            &[("1", "iso-local", "2026-09-01T00:00:00+00:00", None)],
        );
        seed_ordered(&b_path, &[]);
        let (a, a_cfg, a_local, _b, _b_cfg, _b_local) =
            two_nodes_with(&a_path, &b_path, None, None, legion_policy).await;

        // A hears a delta whose ordering column carries an integer Unix time
        // while A stores ISO-8601 text.
        let mut row: HashMap<String, serde_json::Value> = HashMap::new();
        row.insert("id".into(), "1".into());
        row.insert("body".into(), "int-peer".into());
        row.insert("created_at".into(), serde_json::Value::Null);
        row.insert("updated_at".into(), serde_json::json!(1_800_000_000i64));
        row.insert("deleted_at".into(), serde_json::Value::Null);

        let delta = Body::Delta(crate::broadcast::DeltaPacket {
            version: PROTOCOL_VERSION,
            source_id: "node-b".into(),
            seq: 1,
            part: 0,
            total_parts: 1,
            table: "notes".into(),
            upserts: vec![row],
            deletes: Vec::new(),
        });
        let sealed = a.seal(delta).unwrap();
        a.handle(&sealed, &a_local, &a_cfg).await.unwrap();

        let notes = a.ordering_notes().await;
        assert_eq!(notes.len(), 1, "one note per table, got {notes:?}");
        assert_eq!(notes[0].table, "notes");
        assert!(!notes[0].ordering_unavailable);
        assert_eq!(
            notes[0].representation_mismatch,
            vec!["updated_at".to_string()],
            "the disagreeing column must be named"
        );
    }

    /// A `Delta` carrying deletes is parsed and dropped, never applied (#311).
    ///
    /// This pins a deliberate boundary, not a bug to fix later in place.
    /// `broadcast_delta` no longer sends deletes, but a 0.4.x peer still can, and
    /// the row it names must survive: applying a bare delete needs a tombstone,
    /// and without one any peer still holding the row resurrects it on the next
    /// heartbeat -- so dropping it loses less than applying it. If a future
    /// change makes this test fail, tombstone propagation (v0.2) is the thing
    /// that has to land with it.
    #[tokio::test]
    async fn delta_deletes_are_dropped_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "alice"), ("2", "bob")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, _b, _bc, _bl) = two_nodes(&a_path, &b_path).await;

        let delta = Body::Delta(crate::broadcast::DeltaPacket {
            version: PROTOCOL_VERSION,
            source_id: "node-b".into(),
            seq: 1,
            part: 0,
            total_parts: 1,
            table: "users".into(),
            upserts: Vec::new(),
            deletes: vec!["1".to_string()],
        });
        let sealed = a.seal(delta).unwrap();
        let (event, out) = a.handle(&sealed, &a_local, &a_cfg).await.unwrap();

        assert_eq!(
            count(&a_path),
            2,
            "a delete carried on the wire must not remove the row"
        );
        assert_eq!(
            name_of(&a_path, "1").as_deref(),
            Some("alice"),
            "the named row must be untouched, not just present"
        );
        assert!(
            out.is_empty(),
            "a deletes-only delta produces no response datagram"
        );
        assert!(
            matches!(
                event,
                GossipEvent::Applied {
                    rows: 0,
                    rejected: 0,
                    ..
                }
            ),
            "a deletes-only delta applies nothing and rejects nothing, got {event:?}"
        );
    }

    /// A keyless node drops a foreign cluster's sealed datagram, every time.
    ///
    /// This pins the isolation claim in the module doc at the gossip layer, where
    /// an embedder observes it.
    ///
    /// An earlier version of this comment called the test weaker than it is,
    /// claiming it "would have passed under the old sniff too" because the packet
    /// was dropped either way. Review corrected that, and the correction is worth
    /// keeping: the assertion is on `IgnoreReason::Malformed`, and under the old
    /// code a keyless node reported `Undecryptable` for ~127/128 of foreign
    /// datagrams (the sniff rejected them before the parser ever ran). So this
    /// test would have FAILED almost immediately against the old behavior -- it
    /// is a genuine regression test, not merely a guard.
    ///
    /// That mistake came from the same premise #313 itself is about: assuming
    /// "which layer dropped it" is unobservable. `IgnoreReason` is public, so it
    /// is observable, and the reason a packet was dropped is part of the
    /// behavior rather than an implementation detail.
    #[tokio::test]
    async fn keyless_node_drops_sealed_foreign_datagram() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "alice")]);
        seed(&b_path, &[("2", "bob")]);
        // A runs keyless; B is sealed under a key A does not have.
        let key = "b".repeat(64);
        let (a, a_cfg, a_local, b, _b_cfg, _b_local) =
            two_nodes_keyed(&a_path, &b_path, None, Some(&key)).await;

        for i in 0..256 {
            let mut row: HashMap<String, serde_json::Value> = HashMap::new();
            row.insert("id".into(), "2".into());
            row.insert("name".into(), "bob".into());
            let sealed = b
                .seal(Body::Delta(crate::broadcast::DeltaPacket {
                    version: PROTOCOL_VERSION,
                    source_id: "node-b".into(),
                    seq: i + 1,
                    part: 0,
                    total_parts: 1,
                    table: "users".into(),
                    upserts: vec![row],
                    deletes: Vec::new(),
                }))
                .unwrap();
            let (event, out) = a.handle(&sealed, &a_local, &a_cfg).await.unwrap();
            assert!(
                matches!(event, GossipEvent::Ignored(IgnoreReason::Malformed)),
                "iteration {i}: a foreign sealed datagram must be dropped, got {event:?}"
            );
            assert!(
                out.is_empty(),
                "iteration {i}: dropped datagram answers nothing"
            );
        }

        assert_eq!(
            count(&a_path),
            1,
            "no foreign row may ever land in a keyless node's database"
        );
        assert_eq!(name_of(&a_path, "2"), None, "row 2 belongs to B's cluster");
    }

    /// Bind two gossip nodes with distinct instance ids on a test group. They
    /// share no path or DB identity, so convergence proves sync is scoped by
    /// group/key membership alone, never by file location.
    async fn two_nodes(
        a_path: &str,
        b_path: &str,
    ) -> (Gossip, Config, LocalDb, Gossip, Config, LocalDb) {
        two_nodes_keyed(a_path, b_path, None, None).await
    }

    /// Like `two_nodes` but with explicit per-node encryption keys (hex), so
    /// tests can exercise the sealed wire and cross-key isolation.
    async fn two_nodes_keyed(
        a_path: &str,
        b_path: &str,
        key_a: Option<&str>,
        key_b: Option<&str>,
    ) -> (Gossip, Config, LocalDb, Gossip, Config, LocalDb) {
        two_nodes_with(a_path, b_path, key_a, key_b, |_| {}).await
    }

    /// The port every test binds on.
    ///
    /// NOT [`DEFAULT_PORT`]: a test that binds the product's well-known port
    /// fails whenever anything else on the host holds it -- a real smugglr
    /// daemon, or an embedder using the same port for its own mesh -- and the
    /// failure reads as a broken protocol rather than a busy socket.
    const TEST_PORT: u16 = 39337;

    /// Bind two gossip nodes. Keys are per node (they are what asymmetry tests
    /// vary); `tweak` is applied to both, for settings a converging mesh must
    /// share -- the apply policy and its ordering columns.
    async fn two_nodes_with(
        a_path: &str,
        b_path: &str,
        key_a: Option<&str>,
        key_b: Option<&str>,
        tweak: impl Fn(&mut BroadcastConfig),
    ) -> (Gossip, Config, LocalDb, Gossip, Config, LocalDb) {
        let a_cfg = cfg(a_path);
        let b_cfg = cfg(b_path);
        let a_local = LocalDb::open(a_path).unwrap();
        let b_local = LocalDb::open(b_path).unwrap();

        let mut bc_a = BroadcastConfig {
            port: TEST_PORT,
            instance_id: Some("node-a".into()),
            secret: key_a.map(String::from),
            ..Default::default()
        };
        let mut bc_b = BroadcastConfig {
            port: TEST_PORT,
            instance_id: Some("node-b".into()),
            secret: key_b.map(String::from),
            ..Default::default()
        };
        tweak(&mut bc_a);
        tweak(&mut bc_b);

        let group = Ipv4Addr::new(239, 255, 99, 88);
        let a = Gossip::bind(&bc_a, group).await.unwrap();
        let b = Gossip::bind(&bc_b, group).await.unwrap();
        (a, a_cfg, a_local, b, b_cfg, b_local)
    }

    /// Deliver one body emitted by `sender` into `receiver` -- faithful wire
    /// path (seal -> JSON+AEAD -> handle), no socket-delivery dependency.
    async fn route(
        sender: &Gossip,
        body: Body,
        receiver: &Gossip,
        rlocal: &LocalDb,
        rcfg: &Config,
    ) -> (GossipEvent, Vec<Body>) {
        let sealed = sender.seal(body).unwrap();
        receiver.handle(&sealed, rlocal, rcfg).await.unwrap()
    }

    fn only(mut bodies: Vec<Body>) -> Body {
        assert_eq!(bodies.len(), 1, "expected exactly one response body");
        bodies.remove(0)
    }

    // The full masterless cycle: late joiner converges, divergent edit converges
    // (received wins), and a replayed delta is dropped. Deterministic: every
    // datagram is routed by hand, so this proves the protocol independent of
    // whether the host actually delivers multicast.
    #[tokio::test]
    async fn masterless_convergence_late_joiner_and_idempotency() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        // A holds Alice+Bob; B is an empty late joiner with the table only.
        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        // 1) A heartbeats one digest part.
        let mut digests = a.digest_bodies(&a_local, &a_cfg).await.unwrap();
        assert_eq!(digests.len(), 1, "one digest part for a small table");
        let digest = digests.remove(0);

        // 2) B hears it and wants both rows.
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 2, .. }),
            "got {ev:?}"
        );
        let want = only(out);

        // 3) A hears the want and serves both rows.
        let (ev, out) = route(&b, want, &a, &a_local, &a_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Served { rows: 2, .. }),
            "got {ev:?}"
        );
        let delta = only(out);

        // 4) B hears the delta and applies -> converged.
        let (ev, out) = route(&a, delta.clone(), &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Applied { rows: 2, .. }),
            "got {ev:?}"
        );
        assert!(out.is_empty());
        assert_eq!(count(&b_path), 2, "late joiner B converged to A");
        assert_eq!(name_of(&b_path, "1").as_deref(), Some("Alice"));

        // 4b) Replaying the very same delta is dropped by the replay guard.
        let (ev, _) = route(&a, delta, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Ignored(IgnoreReason::Replay)),
            "got {ev:?}"
        );

        // 5) Divergent edit: A changes Alice, re-heartbeats; B pulls and the
        //    received row wins.
        {
            let conn = rusqlite::Connection::open(&a_path).unwrap();
            conn.execute("UPDATE users SET name = 'Alice2' WHERE id = '1'", [])
                .unwrap();
        }
        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 1, .. }),
            "got {ev:?}"
        );
        let (_ev, out) = route(&b, only(out), &a, &a_local, &a_cfg).await;
        let (ev, _) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        assert!(matches!(ev, GossipEvent::Applied { .. }), "got {ev:?}");
        assert_eq!(
            name_of(&b_path, "1").as_deref(),
            Some("Alice2"),
            "divergent row resolves last-received-wins"
        );
    }

    /// Seed a table holding two DISTINCT rows whose `__pk` renders identically.
    ///
    /// The PK column is declared `BLOB`, so it has SQLite affinity NONE and
    /// keeps each value in the storage class it arrived as. Integer `1` and
    /// text `'1'` are different values to the PK unique index (SQLite orders
    /// INTEGER before TEXT, so they never compare equal), which is why both
    /// insert -- but `pk_text_expr` renders a single-column PK as
    /// `CAST(col AS TEXT)` and both render `"1"`. That is the real cross-node
    /// shape: one node minted the key as a number, another as a string.
    fn seed_colliding_pk(path: &str) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, updated_at TEXT);
             INSERT INTO users VALUES (1, 'MintedAsInt', '2026-01-01T00:00:00Z');
             INSERT INTO users VALUES ('1', 'MintedAsText', '2026-01-02T00:00:00Z');",
        )
        .unwrap();
    }

    /// A node whose own table has a duplicate `__pk` must not answer a peer's
    /// digest by pulling that peer's rows.
    ///
    /// The regression this pins is specific and was introduced by #269 itself.
    /// `on_digest` used to funnel EVERY `row_hashes` error into one arm written
    /// for "table absent on a late joiner", which treats the local view as
    /// empty so the node wants everything advertised. Once #269 made
    /// `row_hashes` able to fail for a second reason with opposite meaning, a
    /// node with colliding keys read its own table as empty on every received
    /// digest, requested every peer row, and applied them through `on_delta` --
    /// the one path that bypasses the duplicate-`__pk` guard (#278, deferred).
    /// A refusal became a silent full-table pull down the unguarded path.
    ///
    /// The assertion is on the emitted Want payload rather than on the event or
    /// on the call returning `Err`, because the whole defect is that the wrong
    /// thing happened while every status reported fine: pre-fix this returns
    /// `Ok` with a `GossipEvent::Digest` and a Want naming both of A's rows.
    #[tokio::test]
    async fn duplicate_pk_node_does_not_pull_peer_rows_on_digest() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        // A is a healthy peer advertising two rows; B's own table collides.
        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        seed_colliding_pk(&b_path);
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let sealed = a.seal(digest).unwrap();
        let result = b.handle(&sealed, &b_local, &b_cfg).await;

        // Whatever shape the outcome takes, no Want may leave B: it cannot know
        // which of its rows are missing while its own key space is ambiguous.
        let bodies = match &result {
            Ok((_, out)) => out.clone(),
            Err(_) => Vec::new(),
        };
        let wanted: Vec<String> = bodies
            .iter()
            .flat_map(|body| match body {
                Body::Want(w) => w.pks.clone(),
                _ => Vec::new(),
            })
            .collect();
        assert!(
            wanted.is_empty(),
            "a node with a duplicate __pk must not request peer rows -- those \
             would apply through on_delta, which bypasses the guard; wanted {wanted:?}"
        );

        // And the refusal must be visible rather than absorbed: the daemon's
        // recv loop logs this at `warn`, so an operator sees the collision named.
        match result {
            Err(SyncError::DuplicatePrimaryKey { table, pk, .. }) => {
                assert_eq!(table, "users");
                assert_eq!(pk, "1");
            }
            Err(other) => panic!("expected DuplicatePrimaryKey, got {other:?}"),
            Ok((ev, _)) => panic!("expected the refusal to surface, got event {ev:?}"),
        }
    }

    /// A table with no primary key REFUSES instead of wanting every peer row.
    ///
    /// #332. This is the sibling of the duplicate-`__pk` case above and it was
    /// left in the catch-all: `NoPrimaryKey` read as an empty local view, so a
    /// node whose table cannot be synced at all Wanted everything the peer
    /// advertised, forever, while every status read fine.
    ///
    /// The assertion is on the WANT PAYLOAD rather than on the event, because
    /// the defect is that the wrong thing happens quietly -- an event count
    /// would go green on a node that refused and pulled anyway.
    #[tokio::test]
    async fn a_no_primary_key_table_refuses_instead_of_wanting_everything() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        // B has the table and it has NO primary key. Distinct from the late
        // joiner below, which does not have the table at all -- that one is
        // benign and must keep converging.
        rusqlite::Connection::open(&b_path)
            .unwrap()
            .execute_batch("CREATE TABLE users (id TEXT, name TEXT, updated_at TEXT);")
            .unwrap();
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let sealed = a.seal(digest).unwrap();
        let result = b.handle(&sealed, &b_local, &b_cfg).await;

        let bodies = match &result {
            Ok((_, out)) => out.clone(),
            Err(_) => Vec::new(),
        };
        let wanted: Vec<String> = bodies
            .iter()
            .flat_map(|body| match body {
                Body::Want(w) => w.pks.clone(),
                _ => Vec::new(),
            })
            .collect();
        assert!(
            wanted.is_empty(),
            "a table with no primary key cannot be synced at all, so the node must not \
             request peer rows; wanted {wanted:?}"
        );

        match result {
            Err(SyncError::NoPrimaryKey(table)) => assert_eq!(table, "users"),
            Err(other) => panic!("expected NoPrimaryKey, got {other:?}"),
            Ok((ev, _)) => panic!("expected the misconfiguration to surface, got event {ev:?}"),
        }
    }

    /// A no-PK table surfaces on the SERVE path instead of answering "nothing".
    ///
    /// #415, the `on_want` half. Reporting `Served { rows: 0 }` for a table this
    /// node cannot read makes the fault indistinguishable from "the peer
    /// legitimately needed nothing", and the peer waits forever for rows that
    /// are never coming.
    ///
    /// The Want is built by hand rather than driven from a digest, because the
    /// digest path now refuses first (#332) and would never produce one against
    /// this node.
    #[tokio::test]
    async fn a_no_primary_key_table_surfaces_on_want_instead_of_serving_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        seed(&a_path, &[("1", "Alice")]);
        rusqlite::Connection::open(&b_path)
            .unwrap()
            .execute_batch("CREATE TABLE users (id TEXT, name TEXT, updated_at TEXT);")
            .unwrap();
        let (a, _a_cfg, _a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        let want = Body::Want(WantPacket {
            source_id: a.instance_id.clone(),
            table: "users".to_string(),
            pks: vec!["1".to_string()],
        });
        let sealed = a.seal(want).unwrap();
        let result = b.handle(&sealed, &b_local, &b_cfg).await;

        match result {
            Err(SyncError::NoPrimaryKey(table)) => assert_eq!(table, "users"),
            Err(other) => panic!("expected NoPrimaryKey, got {other:?}"),
            Ok((ev, _)) => panic!(
                "a node that cannot read its own table must not answer as though it had \
                 nothing to give; got {ev:?}"
            ),
        }
    }

    /// A no-PK table surfaces on the APPLY path instead of reporting zero rows.
    ///
    /// #415, the `on_delta` half and the one that matters most. Every error from
    /// `upsert_rows_guarded` was reported as `Applied { rows: 0, rejected: 0 }`,
    /// which is byte-identical to "there was nothing to apply". A node whose
    /// table cannot take writes at all therefore dropped every live delta for
    /// it, forever, while every event it emitted said it was healthy.
    ///
    /// Worse than #332 rather than milder: #332 pulled rows it should not have,
    /// and this one stops converging while reporting success.
    #[tokio::test]
    async fn a_no_primary_key_table_surfaces_on_delta_instead_of_applying_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        seed(&a_path, &[("1", "Alice")]);
        rusqlite::Connection::open(&b_path)
            .unwrap()
            .execute_batch("CREATE TABLE users (id TEXT, name TEXT, updated_at TEXT);")
            .unwrap();
        let (a, _a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        let rows = a_local.get_rows("users", &["1".to_string()]).await.unwrap();
        assert_eq!(rows.len(), 1, "A has the row it is about to send");
        let delta = only(a.delta_bodies("users", rows).unwrap());
        let sealed = a.seal(delta).unwrap();
        let result = b.handle(&sealed, &b_local, &b_cfg).await;

        match result {
            Err(SyncError::NoPrimaryKey(table)) => assert_eq!(table, "users"),
            Err(other) => panic!("expected NoPrimaryKey, got {other:?}"),
            Ok((ev, _)) => panic!(
                "a node that silently drops every delta must not report an apply; got {ev:?}"
            ),
        }
    }

    /// The benign cause keeps its old behavior: a late joiner missing the table
    /// still treats its local view as empty and wants everything. Guards the
    /// narrowing above from being over-read as "any row_hashes error refuses",
    /// which would break late-joiner convergence entirely.
    #[tokio::test]
    async fn missing_table_still_wants_everything_on_digest() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        // B has a database but not the advertised table at all.
        rusqlite::Connection::open(&b_path)
            .unwrap()
            .execute_batch("CREATE TABLE unrelated (id TEXT PRIMARY KEY);")
            .unwrap();
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;

        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 2, .. }),
            "a late joiner must still want every advertised row, got {ev:?}"
        );
        match only(out) {
            Body::Want(w) => assert_eq!(w.pks.len(), 2),
            other => panic!("expected a Want, got {other:?}"),
        }
    }

    // Convergence over the ENCRYPTED wire: the same shared key on both nodes.
    // Proves seal-on-A / decrypt-on-B round-trips real XChaCha20-Poly1305
    // ciphertext -- the production path. The keyless convergence test above
    // exercises only the plaintext passthrough.
    #[tokio::test]
    async fn keyed_convergence() {
        let key = "ab".repeat(32); // 64 hex chars -> valid 256-bit key
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) =
            two_nodes_keyed(&a_path, &b_path, Some(&key), Some(&key)).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 2, .. }),
            "got {ev:?}"
        );
        let (ev, out) = route(&b, only(out), &a, &a_local, &a_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Served { rows: 2, .. }),
            "got {ev:?}"
        );
        let (ev, _) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Applied { rows: 2, .. }),
            "got {ev:?}"
        );
        assert_eq!(count(&b_path), 2, "encrypted convergence");
        assert_eq!(name_of(&b_path, "1").as_deref(), Some("Alice"));
    }

    // Cross-key isolation -- the only isolation mechanism after db_path_hash was
    // removed, so it is load-bearing. A datagram sealed with key K1 is dropped
    // CLEANLY (Ignored(Undecryptable), never an error) by a node holding K2, and
    // that node's data is untouched. If `handle` ever let the decrypt error
    // propagate instead, `route`'s unwrap would panic here.
    #[tokio::test]
    async fn wrong_key_datagram_is_dropped() {
        let k1 = "ab".repeat(32);
        let k2 = "cd".repeat(32);
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "Alice")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) =
            two_nodes_keyed(&a_path, &b_path, Some(&k1), Some(&k2)).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Ignored(IgnoreReason::Undecryptable)),
            "foreign-key datagram must drop cleanly, got {ev:?}"
        );
        assert!(out.is_empty());
        assert_eq!(count(&b_path), 0, "foreign-key node must not converge");
    }

    // Regression for the AEAD-envelope-overhead bug (#143): a delta chunked for
    // multicast, once SEALED (Msg wrapper + XChaCha20 nonce+tag), must stay
    // within SAFE_PACKET_SIZE. split_delta reserves DELTA_WIRE_RESERVE for it;
    // with a key set the seal adds the full 40-byte AEAD overhead.
    #[tokio::test]
    async fn keyed_delta_parts_stay_under_mtu_when_sealed() {
        let key = "ab".repeat(32);
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[]);
        seed(&b_path, &[]);
        let (a, _ac, _al, _b, _bc, _bl) =
            two_nodes_keyed(&a_path, &b_path, Some(&key), Some(&key)).await;

        // Enough rows to force multiple delta parts.
        let rows: Vec<HashMap<String, serde_json::Value>> = (0..400)
            .map(|i| {
                HashMap::from([
                    ("id".to_string(), serde_json::json!(format!("pk-{i:020}"))),
                    ("data".to_string(), serde_json::json!("x".repeat(40))),
                ])
            })
            .collect();
        let bodies = a.delta_bodies("users", rows).unwrap();
        assert!(bodies.len() > 1, "rows should span multiple delta parts");
        for body in bodies {
            let sealed = a.seal(body).unwrap();
            assert!(
                sealed.len() <= SAFE_PACKET_SIZE,
                "sealed delta part is {} bytes, exceeds SAFE_PACKET_SIZE {}",
                sealed.len(),
                SAFE_PACKET_SIZE
            );
        }
    }

    // Live multicast smoke test: proves bind/join/send/recv over a real socket.
    // #[ignore] because multicast loopback delivery is environment-dependent
    // (interface selection, container netns, CI sandboxes). Run on real LAN
    // hardware: `cargo test -p smugglr-core multicast -- --ignored`.
    #[tokio::test]
    #[ignore = "requires multicast-capable loopback; run on real hardware"]
    async fn live_multicast_digest_delivers() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "Alice")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        a.broadcast_digests(&a_local, &a_cfg).await.unwrap();
        let mut recv_buf = vec![0u8; RECV_BUF];
        loop {
            let ev = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                b.recv_and_handle(&mut recv_buf, &b_local, &b_cfg),
            )
            .await
            .expect("multicast recv timed out")
            .expect("recv error");
            if let GossipEvent::Digest { wanted, .. } = ev {
                assert_eq!(wanted, 1, "B should want Alice over the wire");
                break;
            }
        }
    }
}
