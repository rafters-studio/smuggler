//! LAN broadcast primitives: peer discovery, delta serialization, and encryption.
//!
//! The live LAN sync engine is [`crate::multicast`] -- a masterless gossip
//! protocol where membership is key possession, not a designated master. This
//! module is no longer a transport in its own right; it provides two things:
//!
//! 1. **Embedder discovery API** -- [`PeerDiscovery`] / [`Announcement`]: UDP
//!    broadcast announce-and-listen on a configurable port (default: 31337),
//!    with a TTL-pruned peer table. Embedders such as legion consume this to
//!    enumerate instances on the subnet. It is intentionally decoupled from the
//!    multicast wire protocol (see the note on [`Announcement`]).
//! 2. **Shared primitives reused by multicast** -- [`DeltaPacket`] /
//!    [`split_delta`] (table-diff serialization, splitting large deltas to fit
//!    UDP size limits), [`ReplayGuard`] (per-peer sliding-window replay
//!    rejection), and the XChaCha20-Poly1305 helpers
//!    ([`maybe_encrypt`] / [`maybe_decrypt`]).
//!
//! When a pre-shared key is configured, multicast traffic is encrypted with
//! XChaCha20-Poly1305 AEAD. Plaintext and encrypted modes are mutually exclusive.

use crate::config::ConflictResolution;
use crate::error::{Result, SyncError};
use chacha20poly1305::aead::{Aead, NewAead};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Protocol version for UDP packets. Bump on breaking wire changes.
///
/// - v2 introduced the masterless multicast gossip envelope (`multicast::Msg`:
///   `Digest`/`Want`/`Delta`).
/// - v3 removed the `db_path_hash` field from that envelope: membership is the
///   shared key + group, never the database's file path. v2 and v3 envelopes are
///   incompatible, so the version is bumped (v2 was never released).
///
/// Nodes on different versions version-skip each other -- there is no
/// cross-version sync.
pub(crate) const PROTOCOL_VERSION: u8 = 3;

/// Default UDP port for broadcast discovery.
pub const DEFAULT_PORT: u16 = 31337;

/// Default interval between broadcast announcements.
const DEFAULT_INTERVAL_SECS: u64 = 30;

/// Peers not seen within this many intervals are pruned.
const PEER_TTL_MULTIPLIER: u64 = 3;

/// Maximum size of a UDP announcement packet.
const MAX_PACKET_SIZE: usize = 1024;

/// Minimum encrypted packet size: 24-byte nonce + 16-byte Poly1305 tag.
const ENCRYPTION_OVERHEAD: usize = 24 + 16;

/// Configuration for LAN broadcast sync.
#[derive(Debug, Clone, Deserialize)]
pub struct BroadcastConfig {
    /// UDP port for broadcast (default: 31337)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Broadcast interval in seconds (default: 30)
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    /// Instance identity (defaults to hostname)
    pub instance_id: Option<String>,

    /// 256-bit pre-shared key, hex-encoded (64 hex chars).
    /// When set, all multicast traffic is encrypted with XChaCha20-Poly1305
    /// (decoded via [`BroadcastConfig::encryption_key`] and consumed by
    /// [`crate::multicast`]).
    pub secret: Option<String>,

    /// How a received row resolves against an existing local row with the same
    /// primary key. Default: `remote_wins`.
    ///
    /// Deliberately scoped to `[broadcast]` and NOT read from
    /// `[sync].conflict_resolution`. That field defaults to
    /// [`ConflictResolution::LocalWins`], while multicast has always behaved as
    /// `remote_wins`; honoring the sync-scoped value would silently flip every
    /// existing multicast deployment to never-accept-a-peer-row -- a
    /// convergence break shipped as a bugfix. `remote_wins` here preserves the
    /// historical semantics exactly; `newer_wins` is an explicit opt-in.
    ///
    /// `"uuid_v7_wins"` is accepted here too, as a deprecated alias for
    /// `"newer_wins"` -- see [`ConflictResolution::parse_str`] (#431).
    #[serde(default = "default_broadcast_conflict_resolution")]
    pub conflict_resolution: ConflictResolution,

    /// Columns forming the ordering signal for `newer_wins`, compared as the
    /// `max` across whichever of them the table actually has.
    ///
    /// A LIST, not a single column, because an ordering key is frequently a
    /// max over several: legion's is `max(created_at, updated_at, deleted_at)`,
    /// and a tombstone that sets `deleted_at` without bumping `updated_at`
    /// would lose every comparison under a single-column compare. One entry is
    /// exactly the single-column behavior.
    ///
    /// Empty (the default) falls back to `[sync].timestamp_column`, so the
    /// ordering signal has one source of truth until an embedder needs more.
    #[serde(default)]
    pub ordering_columns: Vec<String>,
}

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_interval_secs() -> u64 {
    DEFAULT_INTERVAL_SECS
}

fn default_broadcast_conflict_resolution() -> ConflictResolution {
    ConflictResolution::RemoteWins
}

impl Default for BroadcastConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            interval_secs: DEFAULT_INTERVAL_SECS,
            instance_id: None,
            secret: None,
            conflict_resolution: default_broadcast_conflict_resolution(),
            ordering_columns: Vec::new(),
        }
    }
}

impl BroadcastConfig {
    /// Resolve the instance ID, falling back to hostname.
    pub fn resolve_instance_id(&self) -> String {
        self.instance_id
            .clone()
            .unwrap_or_else(|| hostname().unwrap_or_else(|| "unknown".to_string()))
    }

    /// Peer TTL based on broadcast interval.
    pub fn peer_ttl(&self) -> Duration {
        Duration::from_secs(self.interval_secs.saturating_mul(PEER_TTL_MULTIPLIER))
    }

    /// Parse the hex-encoded secret into a 256-bit key for multicast encryption.
    /// Returns None if no secret is configured.
    pub fn encryption_key(&self) -> Result<Option<[u8; 32]>> {
        match &self.secret {
            None => Ok(None),
            Some(hex_key) => {
                let bytes = hex::decode(hex_key).map_err(|_| {
                    SyncError::Config(
                        "broadcast secret must be 64 hex characters (256-bit key)".to_string(),
                    )
                })?;
                let key: [u8; 32] = bytes.try_into().map_err(|_| {
                    SyncError::Config(
                        "broadcast secret must be 64 hex characters (256-bit key)".to_string(),
                    )
                })?;
                Ok(Some(key))
            }
        }
    }
}

/// A discovered peer on the LAN.
///
/// No `protocol_version` is stored: [`PeerDiscovery::receive_one`] already drops
/// announcements whose version differs from [`PROTOCOL_VERSION`] before
/// `register_peer` is ever called, so every registered peer is on the current
/// protocol. The field was vestigial TCP-era state advertising a version
/// guarantee the table did not enforce (#175).
#[derive(Debug, Clone)]
pub struct Peer {
    /// Unique instance identifier
    pub instance_id: String,
    /// Network address the announcement came from
    pub addr: SocketAddr,
    /// Path hash of the database being synced (to match compatible peers)
    pub db_path_hash: String,
    /// When we last heard from this peer
    pub last_seen: Instant,
}

impl Peer {
    /// Check if this peer has expired based on the given TTL.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.last_seen.elapsed() > ttl
    }
}

/// Announcement packet broadcast over UDP for embedder discovery.
///
/// Kept small to fit in a single UDP datagram with room to spare.
///
/// This is the embedder discovery payload (legion consumes it via
/// [`PeerDiscovery`]). The `db_path_hash` and `sync_port` fields carry TCP-era
/// scoping -- "two instances sync only if their database paths match, over a
/// dedicated TCP port" -- that the masterless multicast model (PROTOCOL_VERSION
/// v3, where membership = key possession) deliberately dropped. They remain on
/// the wire for the discovery use case, but an embedder must NOT assume they
/// mean anything to multicast sync: a peer with a different `db_path_hash` can
/// still be a valid multicast group member, and `sync_port` does not name any
/// live transport.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Announcement {
    /// Protocol version (for forward compatibility)
    pub version: u8,
    /// Unique instance identifier
    pub instance_id: String,
    /// SHA256 hash of the database path (not the path itself, for privacy).
    /// TCP-era scoping; not used by multicast membership -- see the type doc.
    pub db_path_hash: String,
    /// TCP-era sync port; names no live transport under multicast -- see the
    /// type doc.
    pub sync_port: u16,
}

impl Announcement {
    pub fn new(instance_id: String, db_path_hash: String, sync_port: u16) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            instance_id,
            db_path_hash,
            sync_port,
        }
    }

    /// Serialize to bytes for UDP transmission.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| SyncError::Broadcast(format!("serialize: {}", e)))
    }

    /// Deserialize from received UDP bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| SyncError::Broadcast(format!("deserialize: {}", e)))
    }
}

// ---------------------------------------------------------------------------
// XChaCha20-Poly1305 encryption
// ---------------------------------------------------------------------------

/// Encrypt a serialized packet for broadcast.
///
/// Wire format: `[24-byte nonce][ciphertext + 16-byte Poly1305 tag]`
///
/// Reached via [`maybe_encrypt`], which multicast uses to seal every datagram.
fn encrypt_packet(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(key.into());

    let mut nonce_bytes = [0u8; 24];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = XNonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| SyncError::Broadcast("encryption failed".to_string()))?;

    let mut packet = Vec::with_capacity(24 + ciphertext.len());
    packet.extend_from_slice(&nonce_bytes);
    packet.extend_from_slice(&ciphertext);
    Ok(packet)
}

/// Decrypt a received packet.
///
/// Expects wire format: `[24-byte nonce][ciphertext + 16-byte Poly1305 tag]`
///
/// Reached via [`maybe_decrypt`], which multicast uses to open every datagram.
fn decrypt_packet(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if data.len() < ENCRYPTION_OVERHEAD {
        return Err(SyncError::Broadcast(format!(
            "packet too short ({} bytes, minimum {})",
            data.len(),
            ENCRYPTION_OVERHEAD
        )));
    }

    let (nonce_bytes, ciphertext) = data.split_at(24);
    let nonce = XNonce::from_slice(nonce_bytes);
    let cipher = XChaCha20Poly1305::new(key.into());

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| SyncError::Broadcast("authentication failed".to_string()))
}

/// Wrap plaintext in an encryption envelope if a key is provided.
pub fn maybe_encrypt(plaintext: &[u8], key: &Option<[u8; 32]>) -> Result<Vec<u8>> {
    match key {
        Some(k) => encrypt_packet(plaintext, k),
        None => Ok(plaintext.to_vec()),
    }
}

/// Unwrap a potentially encrypted packet. Returns None to signal "drop this packet".
pub fn maybe_decrypt(data: &[u8], key: &Option<[u8; 32]>) -> Result<Option<Vec<u8>>> {
    match key {
        Some(k) => {
            if data.len() < ENCRYPTION_OVERHEAD {
                warn!(
                    "Dropping packet: too short for encrypted mode ({} bytes)",
                    data.len()
                );
                return Ok(None);
            }
            Ok(Some(decrypt_packet(data, k)?))
        }
        None => {
            // No key: nothing can be authenticated, so the datagram is handed up
            // as-is and the message parser is the only filter. A foreign
            // cluster's sealed datagram is ciphertext, which does not
            // deserialize into a `Msg` and is dropped one layer up.
            //
            // Deliberately no first-byte sniff for `{` / `[` (#313). That test
            // guessed "is this plaintext JSON?" against a 24-byte random nonce,
            // so it matched 2 byte values in 256 -- 1 foreign datagram in 128
            // was called plaintext and passed up anyway, and the invariant test
            // failed at the same rate. The guess never changed the outcome for
            // those packets either, since the parser rejected them immediately
            // afterward; it only decided *which layer* dropped them, at random.
            // Deferring to the parser reaches the identical end state
            // deterministically. It also removes the false implication that this
            // arm authenticates anything: in plaintext mode nothing decrypts and
            // nothing is verified, which is a property of running without a key,
            // not something a byte test can recover.
            Ok(Some(data.to_vec()))
        }
    }
}

// ---------------------------------------------------------------------------
// Peer discovery
// ---------------------------------------------------------------------------

/// Manages peer discovery via UDP subnet broadcast.
///
/// This is the embedder discovery API: embedders such as legion construct a
/// `PeerDiscovery`, drive [`PeerDiscovery::discover_once`] (or the
/// announce/receive/prune primitives directly), and read the TTL-pruned peer
/// table. It announces [`Announcement`] packets and is intentionally separate
/// from the masterless multicast sync engine ([`crate::multicast`]); see the
/// note on [`Announcement`] for why its `db_path_hash`/`sync_port` scoping does
/// not carry multicast semantics.
pub struct PeerDiscovery {
    config: BroadcastConfig,
    instance_id: String,
    peers: Arc<RwLock<HashMap<String, Peer>>>,
    socket: UdpSocket,
    announcement: Announcement,
}

impl PeerDiscovery {
    /// Bind the UDP socket and prepare for broadcast.
    pub async fn new(config: BroadcastConfig, db_path_hash: String) -> Result<Self> {
        let instance_id = config.resolve_instance_id();
        let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.port);

        let socket = UdpSocket::bind(bind_addr).await.map_err(|e| {
            SyncError::Broadcast(format!(
                "bind {}:{}: {}",
                Ipv4Addr::UNSPECIFIED,
                config.port,
                e
            ))
        })?;

        socket
            .set_broadcast(true)
            .map_err(|e| SyncError::Broadcast(format!("enable SO_BROADCAST: {}", e)))?;

        let announcement = Announcement::new(instance_id.clone(), db_path_hash, config.port);

        info!(
            "Peer discovery bound to port {}, instance_id={}",
            config.port, instance_id
        );

        Ok(Self {
            config,
            instance_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            socket,
            announcement,
        })
    }

    /// Send a broadcast announcement to all peers on the subnet.
    pub async fn announce(&self) -> Result<()> {
        let data = self.announcement.to_bytes()?;
        let broadcast_addr = SocketAddrV4::new(Ipv4Addr::BROADCAST, self.config.port);

        match self.socket.send_to(&data, broadcast_addr).await {
            Ok(n) => {
                debug!("Broadcast announcement sent ({} bytes)", n);
                Ok(())
            }
            Err(e) => {
                warn!("Broadcast send failed: {}", e);
                Ok(())
            }
        }
    }

    /// Listen for a single announcement from the network.
    pub async fn receive_one(&self) -> Result<Option<(Announcement, SocketAddr)>> {
        let mut buf = [0u8; MAX_PACKET_SIZE];

        let (n, addr) = self
            .socket
            .recv_from(&mut buf)
            .await
            .map_err(|e| SyncError::Broadcast(format!("recv: {}", e)))?;

        let announcement = match Announcement::from_bytes(&buf[..n]) {
            Ok(a) => a,
            Err(e) => {
                debug!("Ignoring malformed packet from {}: {}", addr, e);
                return Ok(None);
            }
        };

        if announcement.instance_id == self.instance_id {
            return Ok(None);
        }

        if announcement.version != PROTOCOL_VERSION {
            debug!(
                "Ignoring peer {} with protocol version {} (ours: {})",
                announcement.instance_id, announcement.version, PROTOCOL_VERSION
            );
            return Ok(None);
        }

        Ok(Some((announcement, addr)))
    }

    /// Update the peer table with a received announcement.
    pub async fn register_peer(&self, announcement: &Announcement, addr: SocketAddr) {
        let peer = Peer {
            instance_id: announcement.instance_id.clone(),
            addr,
            db_path_hash: announcement.db_path_hash.clone(),
            last_seen: Instant::now(),
        };

        let mut peers = self.peers.write().await;
        let is_new = !peers.contains_key(&announcement.instance_id);
        peers.insert(announcement.instance_id.clone(), peer);

        if is_new {
            info!(
                "Discovered new peer: {} at {}",
                announcement.instance_id, addr
            );
        } else {
            debug!("Updated peer: {} at {}", announcement.instance_id, addr);
        }
    }

    /// Remove peers that haven't announced within the TTL window.
    pub async fn prune_expired(&self) -> Vec<String> {
        let ttl = self.config.peer_ttl();
        let mut peers = self.peers.write().await;

        let expired: Vec<String> = peers
            .iter()
            .filter(|(_, p)| p.is_expired(ttl))
            .map(|(id, _)| id.clone())
            .collect();

        for id in &expired {
            info!("Peer expired: {}", id);
            peers.remove(id);
        }

        expired
    }

    /// Get a snapshot of all currently known peers.
    pub async fn peers(&self) -> Vec<Peer> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Run the announce-listen-prune loop for a single cycle.
    pub async fn discover_once(&self, listen_duration: Duration) -> Result<Vec<Peer>> {
        self.announce().await?;

        let deadline = Instant::now() + listen_duration;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.receive_one()).await {
                Ok(Ok(Some((announcement, addr)))) => {
                    self.register_peer(&announcement, addr).await;
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    warn!("Error receiving announcement: {}", e);
                }
                Err(_) => {
                    break;
                }
            }
        }

        self.prune_expired().await;
        Ok(self.peers().await)
    }

    /// Get a shared handle to the peer table for use from other tasks.
    #[allow(dead_code)]
    pub fn peer_table(&self) -> Arc<RwLock<HashMap<String, Peer>>> {
        Arc::clone(&self.peers)
    }

    /// The instance ID of this discovery instance.
    #[allow(dead_code)]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
}

/// Get the system hostname, if available.
fn hostname() -> Option<String> {
    #[cfg(unix)]
    {
        use std::ffi::CStr;
        let mut buf = [0u8; 256];
        let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
        if ret == 0 {
            let cstr = unsafe { CStr::from_ptr(buf.as_ptr() as *const libc::c_char) };
            cstr.to_str().ok().map(String::from)
        } else {
            None
        }
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok()
    }
}

/// Compute a SHA256 hash of a database path for use in announcements.
pub fn hash_db_path(path: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Delta serialization wire protocol
// ---------------------------------------------------------------------------

/// Maximum UDP payload size (65535 - 20 IP header - 8 UDP header).
const MAX_UDP_PAYLOAD: usize = 65507;

/// Conservative packet size to avoid IP fragmentation on most networks.
pub(crate) const SAFE_PACKET_SIZE: usize = 1400;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeltaPacket {
    pub version: u8,
    pub source_id: String,
    pub seq: u64,
    pub part: u16,
    pub total_parts: u16,
    pub table: String,
    pub upserts: Vec<HashMap<String, serde_json::Value>>,
    pub deletes: Vec<String>,
}

#[allow(dead_code)]
impl DeltaPacket {
    pub fn new(source_id: String, seq: u64, table: String) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            source_id,
            seq,
            part: 0,
            total_parts: 1,
            table,
            upserts: Vec::new(),
            deletes: Vec::new(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| SyncError::Broadcast(format!("delta serialize: {}", e)))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| SyncError::Broadcast(format!("delta deserialize: {}", e)))
    }

    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty()
    }
}

/// Split a delta into datagram-sized parts.
///
/// `reserve` is wire-envelope headroom (bytes) the caller will add AFTER this
/// returns -- e.g. the multicast `Msg` JSON wrapper plus the XChaCha20 nonce+tag.
/// Each part's bare serialized size is kept at or below `SAFE_PACKET_SIZE -
/// reserve`, so the sealed datagram stays within the safe MTU. Pass 0 if the
/// part goes on the wire as-is.
pub fn split_delta(
    source_id: &str,
    seq: u64,
    table: &str,
    upserts: Vec<HashMap<String, serde_json::Value>>,
    deletes: Vec<String>,
    reserve: usize,
) -> Result<Vec<DeltaPacket>> {
    let limit = SAFE_PACKET_SIZE.saturating_sub(reserve);
    let mut base = DeltaPacket {
        version: PROTOCOL_VERSION,
        source_id: source_id.to_string(),
        seq,
        part: 0,
        total_parts: 1,
        table: table.to_string(),
        upserts: Vec::new(),
        deletes,
    };

    if upserts.is_empty() {
        return Ok(vec![base]);
    }

    base.upserts = upserts.clone();
    let serialized = base.to_bytes()?;
    if serialized.len() <= limit {
        return Ok(vec![base]);
    }

    let mut packets: Vec<DeltaPacket> = Vec::new();
    let mut current = DeltaPacket {
        version: PROTOCOL_VERSION,
        source_id: source_id.to_string(),
        seq,
        part: 0,
        total_parts: 0,
        table: table.to_string(),
        upserts: Vec::new(),
        deletes: base.deletes.clone(),
    };

    for row in upserts {
        current.upserts.push(row);

        let size = current.to_bytes()?.len();
        if size > limit && current.upserts.len() > 1 {
            let overflow = current.upserts.pop().unwrap();
            packets.push(current);

            current = DeltaPacket {
                version: PROTOCOL_VERSION,
                source_id: source_id.to_string(),
                seq,
                part: packets.len() as u16,
                total_parts: 0,
                table: table.to_string(),
                upserts: vec![overflow],
                deletes: Vec::new(),
            };
        }
    }

    if !current.upserts.is_empty() || !current.deletes.is_empty() {
        current.part = packets.len() as u16;
        packets.push(current);
    }

    let total = packets.len() as u16;
    for p in &mut packets {
        p.total_parts = total;
    }

    for p in &packets {
        let size = p.to_bytes()?.len();
        if size > MAX_UDP_PAYLOAD {
            return Err(SyncError::Broadcast(format!(
                "single row exceeds UDP payload limit ({} bytes for table '{}')",
                size, table
            )));
        }
    }

    Ok(packets)
}

/// Hard cap on distinct peers tracked at once. When full, the
/// least-recently-seen peer is evicted to admit a new one (rather than rejecting
/// the newcomer), so a masterless LAN with churning instance_ids never locks out
/// live peers. See #174.
const REPLAY_PEER_CAP: usize = 256;

/// Per-peer replay window plus a last-seen ordinal for LRU eviction.
#[derive(Debug, Clone, Copy)]
struct Window {
    /// Highest sequence number seen from this peer.
    highest: u64,
    /// Bitfield of recently seen sequences below `highest`.
    bitfield: u64,
    /// Monotonic ordinal of the most recent `check` that touched this peer,
    /// used only to pick the least-recently-seen peer to evict at the cap.
    last_seen: u64,
}

/// Tracks seen sequence numbers per peer to detect replay attacks.
///
/// Uses a sliding window of 64 sequence numbers per peer. Packets with
/// previously seen or too-old sequence numbers are rejected. The peer table is
/// capped at [`REPLAY_PEER_CAP`]; at the cap the least-recently-seen peer is
/// evicted so a fresh peer is always admitted (no permanent lockout, #174).
#[derive(Debug)]
pub struct ReplayGuard {
    /// Per-peer sliding window of seen sequences keyed by source_id.
    windows: HashMap<String, Window>,
    /// Monotonic clock stamped onto each touched peer's `last_seen`.
    clock: u64,
}

impl ReplayGuard {
    pub fn new() -> Self {
        Self {
            windows: HashMap::new(),
            clock: 0,
        }
    }

    /// Remove peers not in the given active set to prevent unbounded growth.
    pub fn prune(&mut self, active_peers: &[&str]) {
        self.windows
            .retain(|id, _| active_peers.contains(&id.as_str()));
    }

    /// Evict the least-recently-seen peer to make room for a newcomer. Returns
    /// the evicted source_id, if any.
    fn evict_lru(&mut self) -> Option<String> {
        let victim = self
            .windows
            .iter()
            .min_by_key(|(_, w)| w.last_seen)
            .map(|(id, _)| id.clone())?;
        self.windows.remove(&victim);
        Some(victim)
    }

    /// Check whether a packet with the given source_id and seq should be accepted.
    /// Returns `true` if accepted (not a replay), `false` if rejected.
    pub fn check(&mut self, source_id: &str, seq: u64) -> bool {
        // Cap source_id length to prevent DoS via crafted packets.
        if source_id.len() > 128 {
            warn!(
                "Replay guard: rejecting oversized source_id ({}B)",
                source_id.len()
            );
            return false;
        }
        // At the peer cap, evict the least-recently-seen peer rather than reject
        // the newcomer, so churning instance_ids cannot lock out live peers.
        if !self.windows.contains_key(source_id) && self.windows.len() >= REPLAY_PEER_CAP {
            if let Some(victim) = self.evict_lru() {
                debug!(
                    "Replay guard: peer cap reached, evicting least-recently-seen '{}' for '{}'",
                    victim, source_id
                );
            }
        }

        self.clock += 1;
        let now = self.clock;

        let entry = self.windows.get_mut(source_id);
        match entry {
            None => {
                self.windows.insert(
                    source_id.to_string(),
                    Window {
                        highest: seq,
                        bitfield: 0,
                        last_seen: now,
                    },
                );
                true
            }
            Some(Window {
                highest,
                bitfield,
                last_seen,
            }) => {
                *last_seen = now;
                if seq > *highest {
                    let shift = seq - *highest;
                    if shift < 64 {
                        *bitfield = (*bitfield << shift) | (1 << (shift - 1));
                    } else {
                        *bitfield = 0;
                    }
                    *highest = seq;
                    true
                } else if seq == *highest {
                    // Exact duplicate of the highest seen
                    debug!(
                        "Replay guard: dropping duplicate seq {} from '{}'",
                        seq, source_id
                    );
                    false
                } else if *highest - seq > 63 {
                    // Too old -- outside the 64-seq window
                    warn!(
                        "Replay guard: dropping packet from '{}' with seq {} (too old, highest={})",
                        source_id, seq, *highest
                    );
                    false
                } else {
                    let offset = *highest - seq - 1;
                    let bit = 1u64 << offset;
                    if *bitfield & bit != 0 {
                        debug!(
                            "Replay guard: dropping duplicate seq {} from '{}'",
                            seq, source_id
                        );
                        false
                    } else {
                        *bitfield |= bit;
                        true
                    }
                }
            }
        }
    }
}

impl Default for ReplayGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve the PID lock file path for the broadcast daemon.
pub fn broadcast_pid_lock_path(config_path: &std::path::Path) -> std::path::PathBuf {
    if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            std::path::PathBuf::from(".smugglr-broadcast.pid")
        } else {
            parent.join(".smugglr-broadcast.pid")
        }
    } else {
        std::path::PathBuf::from(".smugglr-broadcast.pid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two default paths must agree. `BroadcastConfig` carries a
    /// hand-written `impl Default` AND per-field `#[serde(default = ...)]`; a
    /// config file that omits a key takes the serde path, while an embedder
    /// constructing the struct takes the other. A disagreement would mean the
    /// same deployment resolves conflicts differently depending on whether the
    /// key was written down.
    #[test]
    fn omitted_conflict_keys_deserialize_to_the_struct_defaults() {
        let from_toml: BroadcastConfig = toml::from_str("").expect("empty [broadcast] table");
        let from_struct = BroadcastConfig::default();

        assert_eq!(
            from_toml.conflict_resolution, from_struct.conflict_resolution,
            "serde default and impl Default disagree on conflict_resolution"
        );
        assert_eq!(from_toml.ordering_columns, from_struct.ordering_columns);
        assert_eq!(
            from_toml.conflict_resolution,
            ConflictResolution::RemoteWins,
            "an existing deployment must keep last-received-wins"
        );
    }

    /// The opt-in path: legion's shape, spelled the way an operator writes it.
    #[test]
    fn newer_wins_and_an_ordering_list_parse_from_toml() {
        let bc: BroadcastConfig = toml::from_str(
            r#"
            conflict_resolution = "newer_wins"
            ordering_columns = ["created_at", "updated_at", "deleted_at"]
            "#,
        )
        .expect("parse");
        assert_eq!(bc.conflict_resolution, ConflictResolution::NewerWins);
        assert_eq!(
            bc.ordering_columns,
            vec!["created_at", "updated_at", "deleted_at"]
        );
    }

    #[test]
    fn test_announcement_roundtrip() {
        let original =
            Announcement::new("test-machine".to_string(), "abc123hash".to_string(), 31337);
        let bytes = original.to_bytes().expect("serialize");
        let decoded = Announcement::from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    #[test]
    fn peer_ttl_saturates_instead_of_overflowing() {
        // Regression for #194: interval_secs is operator-controlled with no upper
        // bound, so interval_secs * PEER_TTL_MULTIPLIER can overflow u64 --
        // panicking in debug, wrapping to a tiny TTL in release. saturating_mul
        // pins it to the max instead.
        let bc = BroadcastConfig {
            interval_secs: u64::MAX,
            ..BroadcastConfig::default()
        };
        assert_eq!(bc.peer_ttl(), Duration::from_secs(u64::MAX));

        let bc = BroadcastConfig {
            interval_secs: 30,
            ..BroadcastConfig::default()
        };
        assert_eq!(bc.peer_ttl(), Duration::from_secs(30 * PEER_TTL_MULTIPLIER));
    }

    #[test]
    fn test_announcement_fits_in_udp() {
        let announcement = Announcement::new("a".repeat(128), "f".repeat(64), 31337);
        let bytes = announcement.to_bytes().expect("serialize");
        assert!(bytes.len() < MAX_PACKET_SIZE);
    }

    #[test]
    fn test_announcement_rejects_garbage() {
        let result = Announcement::from_bytes(b"not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_db_path_deterministic() {
        let h1 = hash_db_path("/home/user/legion.db");
        let h2 = hash_db_path("/home/user/legion.db");
        assert_eq!(h1, h2);
        let h3 = hash_db_path("/home/other/legion.db");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_peer_expiry() {
        let peer = Peer {
            instance_id: "test".to_string(),
            addr: "127.0.0.1:31337".parse().unwrap(),
            db_path_hash: "abc".to_string(),
            last_seen: Instant::now() - Duration::from_secs(100),
        };
        assert!(peer.is_expired(Duration::from_secs(90)));
        assert!(!peer.is_expired(Duration::from_secs(110)));
    }

    #[test]
    fn test_broadcast_config_defaults() {
        let config = BroadcastConfig::default();
        assert_eq!(config.port, 31337);
        assert_eq!(config.interval_secs, 30);
        assert!(config.instance_id.is_none());
        assert!(config.secret.is_none());
        assert_eq!(config.peer_ttl(), Duration::from_secs(90));
    }

    #[tokio::test]
    async fn test_peer_discovery_loopback() {
        let config_a = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            instance_id: Some("machine-a".to_string()),
            ..Default::default()
        };
        let config_b = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            instance_id: Some("machine-b".to_string()),
            ..Default::default()
        };

        let hash = hash_db_path("/test/legion.db");
        let discovery_a = PeerDiscovery::new(config_a, hash.clone()).await.unwrap();
        let discovery_b = PeerDiscovery::new(config_b, hash.clone()).await.unwrap();

        let _port_a = discovery_a.socket.local_addr().unwrap().port();
        let port_b = discovery_b.socket.local_addr().unwrap().port();

        let announcement_a = discovery_a.announcement.to_bytes().unwrap();
        let addr_b = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port_b);
        discovery_a
            .socket
            .send_to(&announcement_a, addr_b)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), discovery_b.receive_one())
            .await
            .expect("timeout")
            .expect("receive");

        let (announcement, _addr) = result.expect("should have received announcement");
        assert_eq!(announcement.instance_id, "machine-a");
        assert_eq!(announcement.db_path_hash, hash);
    }

    #[tokio::test]
    async fn test_register_and_prune_peers() {
        let config = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            instance_id: Some("test-host".to_string()),
            ..Default::default()
        };

        let hash = hash_db_path("/test/db.sqlite");
        let discovery = PeerDiscovery::new(config, hash.clone()).await.unwrap();

        let announcement = Announcement::new("remote-peer".to_string(), hash.clone(), 31337);
        let addr: SocketAddr = "192.168.1.100:31337".parse().unwrap();
        discovery.register_peer(&announcement, addr).await;

        let peers = discovery.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].instance_id, "remote-peer");
    }

    // -----------------------------------------------------------------------
    // Delta protocol tests
    // -----------------------------------------------------------------------

    fn make_row(id: &str, name: &str) -> HashMap<String, serde_json::Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), serde_json::Value::String(id.to_string()));
        row.insert(
            "name".to_string(),
            serde_json::Value::String(name.to_string()),
        );
        row
    }

    #[test]
    fn test_delta_packet_roundtrip() {
        let mut packet = DeltaPacket::new("machine-a".to_string(), 1, "users".to_string());
        packet.upserts.push(make_row("1", "Alice"));
        packet.deletes.push("2".to_string());
        let bytes = packet.to_bytes().expect("serialize");
        let decoded = DeltaPacket::from_bytes(&bytes).expect("deserialize");
        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_delta_packet_empty() {
        let packet = DeltaPacket::new("machine-a".to_string(), 0, "users".to_string());
        assert!(packet.is_empty());
        let mut non_empty = packet.clone();
        non_empty.upserts.push(make_row("1", "Bob"));
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_split_delta_small_fits_one_packet() {
        let upserts = vec![make_row("1", "Alice"), make_row("2", "Bob")];
        let deletes = vec!["3".to_string()];
        let packets =
            split_delta("machine-a", 1, "users", upserts.clone(), deletes.clone(), 0).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].upserts.len(), 2);
        assert_eq!(packets[0].deletes.len(), 1);
        assert_eq!(packets[0].part, 0);
        assert_eq!(packets[0].total_parts, 1);
    }

    #[test]
    fn test_split_delta_large_splits() {
        let big_value = "x".repeat(500);
        let upserts: Vec<HashMap<String, serde_json::Value>> = (0..20)
            .map(|i| {
                let mut row = HashMap::new();
                row.insert("id".to_string(), serde_json::Value::String(i.to_string()));
                row.insert(
                    "data".to_string(),
                    serde_json::Value::String(big_value.clone()),
                );
                row
            })
            .collect();

        let packets = split_delta("machine-a", 5, "big_table", upserts, vec![], 0).unwrap();
        assert!(packets.len() > 1, "should split into multiple packets");

        for p in &packets {
            assert_eq!(p.seq, 5);
            assert_eq!(p.table, "big_table");
            assert_eq!(p.total_parts, packets.len() as u16);
        }

        for (i, p) in packets.iter().enumerate() {
            assert_eq!(p.part, i as u16);
        }

        for p in &packets {
            let size = p.to_bytes().unwrap().len();
            assert!(
                size <= SAFE_PACKET_SIZE || p.upserts.len() == 1,
                "packet too large: {} bytes with {} rows",
                size,
                p.upserts.len()
            );
        }
    }

    #[test]
    fn test_split_delta_empty() {
        let packets = split_delta("machine-a", 0, "empty_table", vec![], vec![], 0).unwrap();
        assert_eq!(packets.len(), 1);
        assert!(packets[0].is_empty());
    }

    // -----------------------------------------------------------------------
    // Encryption tests
    // -----------------------------------------------------------------------

    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte = i as u8;
        }
        key
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let plaintext = b"hello smuggler broadcast";
        let encrypted = encrypt_packet(plaintext, &key).expect("encrypt");
        let decrypted = decrypt_packet(&encrypted, &key).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_wrong_key_fails_authentication() {
        let key_a = test_key();
        let mut key_b = test_key();
        key_b[0] = 0xFF;
        let encrypted = encrypt_packet(b"secret data", &key_a).expect("encrypt");
        let result = decrypt_packet(&encrypted, &key_b);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("authentication failed"), "got: {}", err);
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let key = test_key();
        let mut encrypted = encrypt_packet(b"important data", &key).expect("encrypt");
        encrypted[30] ^= 0xFF;
        let result = decrypt_packet(&encrypted, &key);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("authentication failed"), "got: {}", err);
    }

    #[test]
    fn test_nonce_uniqueness() {
        let key = test_key();
        let plaintext = b"same plaintext twice";
        let encrypted_1 = encrypt_packet(plaintext, &key).expect("encrypt 1");
        let encrypted_2 = encrypt_packet(plaintext, &key).expect("encrypt 2");
        assert_ne!(encrypted_1, encrypted_2);
        assert_eq!(decrypt_packet(&encrypted_1, &key).unwrap(), plaintext);
        assert_eq!(decrypt_packet(&encrypted_2, &key).unwrap(), plaintext);
    }

    #[test]
    fn test_packet_too_short() {
        let key = test_key();
        let short_data = vec![0u8; 20];
        let result = decrypt_packet(&short_data, &key);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("packet too short"), "got: {}", err);
    }

    #[test]
    fn test_maybe_encrypt_decrypt_with_key() {
        let key = Some(test_key());
        let plaintext = b"broadcast payload";
        let encrypted = maybe_encrypt(plaintext, &key).expect("encrypt");
        assert_ne!(&encrypted[..], &plaintext[..]);
        let decrypted = maybe_decrypt(&encrypted, &key)
            .expect("decrypt")
            .expect("should not be dropped");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_maybe_encrypt_decrypt_without_key() {
        let plaintext = b"{\"version\":1}";
        let result = maybe_encrypt(plaintext, &None).expect("passthrough");
        assert_eq!(&result[..], &plaintext[..]);
        let decrypted = maybe_decrypt(&result, &None)
            .expect("passthrough")
            .expect("should not be dropped");
        assert_eq!(decrypted, plaintext);
    }

    /// A keyless node classifies nothing, and does so deterministically (#313).
    ///
    /// This replaces `test_plaintext_mode_drops_encrypted_packet`, which asserted
    /// that `maybe_decrypt` itself returned `None` for a sealed datagram. It did
    /// that by sniffing the first byte for `{` or `[` -- a guess against a 24-byte
    /// random nonce, matching 2 byte values in 256, so the old assertion failed
    /// for 1 packet in 128 (measured 6/1000). The guess never changed the end
    /// state for the packets it let through, since the `Msg` parser rejected them
    /// immediately after; it only decided which layer dropped them, at random.
    ///
    /// The contract now is that this arm makes no decision at all, so the test is
    /// the inverse: every datagram is handed up unchanged, whatever its first
    /// byte. Looped over fresh random nonces so the assertion is about the whole
    /// byte distribution rather than one lucky sample. Under the old code this
    /// loop fails on its *first* iteration with probability 254/256 = 127/128 --
    /// the old arm returned `Some` only for the 2 byte values in 256 it
    /// recognized, which is the same coin the old test was losing 2/256 = 1/128
    /// of the time by flipping it the other way.
    #[test]
    fn plaintext_mode_classifies_nothing_and_is_not_probabilistic() {
        let key = test_key();
        for i in 0..2_000 {
            let encrypted = encrypt_packet(b"secret", &key).expect("encrypt");
            let out = maybe_decrypt(&encrypted, &None)
                .expect("a keyless node never errors on a datagram")
                .expect("a keyless node passes every datagram up, whatever its first byte");
            assert_eq!(
                out, encrypted,
                "iteration {i}: bytes must be handed up unchanged, not filtered here"
            );
        }
    }

    #[test]
    fn test_encrypted_mode_drops_short_plaintext_packet() {
        let key = Some(test_key());
        let plaintext = b"{\"v\":1}";
        let result = maybe_decrypt(plaintext, &key).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_encryption_key_parsing() {
        let config = BroadcastConfig {
            secret: Some("a".repeat(64)),
            ..Default::default()
        };
        let key = config.encryption_key().unwrap();
        assert!(key.is_some());
        assert_eq!(key.unwrap(), [0xAA; 32]);

        let config = BroadcastConfig::default();
        assert!(config.encryption_key().unwrap().is_none());

        let config = BroadcastConfig {
            secret: Some("not-hex".to_string()),
            ..Default::default()
        };
        assert!(config.encryption_key().is_err());

        let config = BroadcastConfig {
            secret: Some("aabb".to_string()),
            ..Default::default()
        };
        assert!(config.encryption_key().is_err());
    }

    #[test]
    fn test_announcement_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let announcement =
            Announcement::new("test-machine".to_string(), "abc123hash".to_string(), 31337);
        let plaintext = announcement.to_bytes().expect("serialize");
        let encrypted = encrypt_packet(&plaintext, &key).expect("encrypt");
        let decrypted = decrypt_packet(&encrypted, &key).expect("decrypt");
        let decoded = Announcement::from_bytes(&decrypted).expect("deserialize");
        assert_eq!(announcement, decoded);
    }

    #[test]
    fn test_delta_packet_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let mut packet = DeltaPacket::new("machine-a".to_string(), 1, "users".to_string());
        packet.upserts.push(make_row("1", "Alice"));
        packet.deletes.push("2".to_string());
        let plaintext = packet.to_bytes().expect("serialize");
        let encrypted = encrypt_packet(&plaintext, &key).expect("encrypt");
        let decrypted = decrypt_packet(&encrypted, &key).expect("decrypt");
        let decoded = DeltaPacket::from_bytes(&decrypted).expect("deserialize");
        assert_eq!(packet, decoded);
    }

    // -----------------------------------------------------------------------
    // ReplayGuard tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_replay_guard_accepts_new_sequence() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 5));
    }

    #[test]
    fn test_replay_guard_rejects_duplicate() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 5));
        assert!(!guard.check("peer-a", 5));
    }

    #[test]
    fn test_replay_guard_sliding_window() {
        let mut guard = ReplayGuard::new();
        // Accept seq 100
        assert!(guard.check("peer-a", 100));
        // Seq 50 is too old (100 - 50 = 50 < 64, but let's use 36 which is outside)
        // Actually 100 - 50 = 50 which is < 64, so it's within the window
        assert!(guard.check("peer-a", 50));
        // But seq 50 again is a duplicate
        assert!(!guard.check("peer-a", 50));
        // Seq 30 is within window (100 - 30 = 70 >= 64), so too old
        assert!(!guard.check("peer-a", 30));
        // Seq 80 is within window and not seen
        assert!(guard.check("peer-a", 80));
    }

    #[test]
    fn test_replay_guard_too_old_rejected() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 100));
        // 100 - 36 = 64, which is outside the 64-seq window (> 63)
        assert!(!guard.check("peer-a", 36));
        // 100 - 37 = 63, exactly at boundary, within window
        assert!(guard.check("peer-a", 37));
    }

    #[test]
    fn test_replay_guard_independent_peers() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 5));
        assert!(guard.check("peer-b", 5));
        // Each peer has independent state
        assert!(!guard.check("peer-a", 5));
        assert!(!guard.check("peer-b", 5));
    }

    #[test]
    fn test_replay_guard_advancing_window() {
        let mut guard = ReplayGuard::new();
        // Sequential acceptance
        for seq in 0..10 {
            assert!(guard.check("peer-a", seq));
        }
        // All duplicates rejected
        for seq in 0..10 {
            assert!(!guard.check("peer-a", seq));
        }
    }

    #[test]
    fn test_replay_guard_large_gap_resets_bitfield() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 0));
        // Jump far ahead (gap >= 64 resets bitfield)
        assert!(guard.check("peer-a", 200));
        // Old seq 0 is too old now
        assert!(!guard.check("peer-a", 0));
        // But seq 200 was already seen (it's the highest)
        assert!(!guard.check("peer-a", 200));
        // Recent within window should work
        assert!(guard.check("peer-a", 199));
    }

    #[test]
    fn test_replay_guard_prune_removes_stale_peers() {
        let mut guard = ReplayGuard::new();
        guard.check("peer-a", 1);
        guard.check("peer-b", 1);
        guard.check("peer-c", 1);
        assert_eq!(guard.windows.len(), 3);

        guard.prune(&["peer-a", "peer-c"]);
        assert_eq!(guard.windows.len(), 2);
        assert!(guard.windows.contains_key("peer-a"));
        assert!(!guard.windows.contains_key("peer-b"));
        assert!(guard.windows.contains_key("peer-c"));
    }

    #[test]
    fn test_replay_guard_default() {
        let guard = ReplayGuard::default();
        assert!(guard.windows.is_empty());
    }

    /// Regression for #174: at the peer cap, a brand-new peer must be admitted
    /// by evicting the least-recently-seen peer, not rejected. Before the fix,
    /// `check` returned false for every new source_id once the table hit 256,
    /// permanently locking out fresh peers on a churning LAN.
    #[test]
    fn test_replay_guard_evicts_lru_instead_of_rejecting_new_peer() {
        let mut guard = ReplayGuard::new();

        // Fill to the cap. peer-0 is touched first, so it is the LRU.
        for i in 0..REPLAY_PEER_CAP {
            assert!(guard.check(&format!("peer-{i}"), 1));
        }
        assert_eq!(guard.windows.len(), REPLAY_PEER_CAP);

        // Re-touch every peer except peer-0 so peer-0 stays the LRU.
        for i in 1..REPLAY_PEER_CAP {
            assert!(guard.check(&format!("peer-{i}"), 2));
        }

        // A fresh peer at the cap must be ACCEPTED (the bug returned false here).
        assert!(
            guard.check("fresh-peer", 1),
            "new peer must be admitted at the cap, not locked out"
        );

        // The table stays bounded and the LRU peer-0 was the one evicted.
        assert_eq!(guard.windows.len(), REPLAY_PEER_CAP);
        assert!(!guard.windows.contains_key("peer-0"));
        assert!(guard.windows.contains_key("fresh-peer"));
    }
}
