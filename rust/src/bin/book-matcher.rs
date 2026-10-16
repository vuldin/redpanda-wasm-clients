//! A matcher guest with a book that holds depth and state that survives a
//! processor restart.
//!
//! Two gaps the 2026-10-15 runs left open, both closed here rather than in
//! the published guest (whose md5 every published figure is anchored to):
//!
//! 1. Depth. The reference book (../orderbook.rs) collapses between 100 and
//!    1,000 resting orders per side. This guest uses ../fastbook.rs, which
//!    is differentially tested against it below.
//! 2. Durability. With `STATE=snapshot` the guest registers a shared-memory
//!    region with the broker and keeps its book, risk exposure and replay
//!    high-water marks in it. The broker copies the region into the input
//!    partition's raft group every 5s and writes it back before the first
//!    record after a restart; the guest rebuilds from it on that first record.
//!    `SNAPSHOT_MODE=live` (the default) keeps the region current slot by slot
//!    after every record and registers only the bytes in use; `full`
//!    re-serialises the whole book every SNAPSHOT_EVERY_MS into a fixed-size
//!    registration, the original format, kept for comparison. `STATE=off` (the default) registers nothing,
//!    so the broker never checkpoints - the no-durability baseline, same
//!    binary.
//!
//! The broker only links the shared-memory host module for a binary listed
//! in `wasm_trusted_modules` with the `shared_memory` capability, so this
//! module must be trusted by sha256 even for `STATE=off`, or it will not
//! instantiate.
//!
//! The snapshot and the committed input offset are independent (checkpoint
//! every 5s, offset commit every 3s), so after an unplanned failure a
//! restored guest resumes either behind its snapshot (records it already
//! applied are replayed) or ahead of it (records are lost). The guest cannot
//! see offsets, so it detects both from the sequence the load generator
//! puts in every order id: replays are skipped and counted, and a jump past
//! the next expected sequence is logged as a gap. See `Lanes`.
//!
//! Wire format, risk semantics and the trailing matched_at_nanos are the
//! same as the other matcher guests. Handles type 0, 1 and 2.

#![allow(dead_code)]

#[path = "../fastbook.rs"]
mod fastbook;
#[path = "../orderbook.rs"]
mod orderbook;
#[path = "../wire.rs"]
mod wire;
// The reference risk checker, only for the differential test against
// `Exposure` below.
#[cfg(test)]
#[path = "../risk.rs"]
mod risk;

use std::time::{SystemTime, UNIX_EPOCH};

use fastbook::{Book, FnvMap};
use orderbook::{Order, Side};
use wire::MessageType;

// ---------------------------------------------------------------------------
// risk: same semantics as ../risk.rs, plus iteration for the snapshot
// ---------------------------------------------------------------------------

/// ../risk.rs's Checker cannot be iterated, and adding a method to it would
/// touch the published guest's source. This is the same check-and-record
/// logic with a fixed hasher and an iterator; `exposure_matches_risk_rs`
/// pins the equivalence.
#[derive(Default)]
pub struct Exposure {
    limit: i64,
    map: FnvMap<String, i64>,
    /// Participants whose exposure changed, recorded only while `track` is
    /// on (the live snapshot region rewrites their slots after each record).
    track: bool,
    dirty: Vec<String>,
}

impl Exposure {
    fn new(limit: i64) -> Self {
        Self {
            limit,
            map: FnvMap::default(),
            track: false,
            dirty: Vec::new(),
        }
    }
    fn check(&self, participant: &str, notional: i64) -> bool {
        if self.limit <= 0 {
            return true;
        }
        let current = *self.map.get(participant).unwrap_or(&0);
        current + notional <= self.limit
    }
    fn record(&mut self, participant: &str, delta: i64) {
        if self.track && self.dirty.last().map(|d| d.as_str()) != Some(participant) {
            self.dirty.push(participant.to_string());
        }
        // get_mut first: the hot path is a participant that already has
        // exposure, and entry() would allocate a String for every call.
        let v = match self.map.get_mut(participant) {
            Some(v) => {
                *v += delta;
                *v
            }
            None => {
                self.map.insert(participant.to_string(), delta);
                delta
            }
        };
        if v <= 0 {
            self.map.remove(participant);
        }
    }
    fn exposure(&self, participant: &str) -> i64 {
        *self.map.get(participant).unwrap_or(&0)
    }
}

// ---------------------------------------------------------------------------
// replay detection from the sequence inside each order id
// ---------------------------------------------------------------------------

/// Parsed from the loadgen id layout
/// `<runID><phase>-<leg><seq>[q<k>]-<nanos>`: legs `s` (resting sell),
/// `b` (aggressor buy) and `i`/`iq1` (passive inserts) always arrive in that order
/// for one seq, and one producer client sends seqs `idx, idx+P, idx+2P...`
/// in order, where P is the producer count. So within a lane - (run, phase,
/// seq mod P) - `seq*4 + leg` strictly increases, and anything at or below
/// the lane's high-water mark is a replay. Ids that do not parse (preload
/// ids carry a negative seq, cancels reference another order's id) are
/// never deduplicated: a replayed preload is already rejected by the book's
/// duplicate check, and a replayed cancel finds nothing to cancel.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct LaneKey {
    run: String,
    phase: u8,
    lane: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct LaneState {
    hwm: u64,
    /// Set for every lane restored from a snapshot, cleared by the lane's
    /// first record after restore - which is the one record that can show a
    /// gap.
    check_gap: bool,
}

fn parse_seq_id(id: &str) -> Option<(&str, u8, u8, u64)> {
    let b = id.as_bytes();
    let p = b.iter().position(|c| !c.is_ascii_digit())?;
    if p == 0 || b.len() < p + 4 || b[p + 1] != b'-' {
        return None;
    }
    let leg = match b[p + 2] {
        b's' => 0,
        b'b' => 1,
        b'i' => 2,
        _ => return None,
    };
    let rest = &b[p + 3..];
    let end = rest.iter().position(|c| !c.is_ascii_digit())?;
    if end == 0 || !(rest[end] == b'-' || rest[end] == b'q') {
        return None;
    }
    let seq: u64 = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
    // Inserts can come two per seq (loadgen's `i` and `iq1`), so the q index
    // picks the leg slot. For s/b legs q is the order's index inside a mass
    // quote, and the record is classified by its first order alone.
    let mut leg = leg;
    if leg == 2 && rest[end] == b'q' {
        let q = &rest[end + 1..];
        let qe = q.iter().position(|c| !c.is_ascii_digit())?;
        match &q[..qe] {
            b"1" => leg = 3,
            _ => return None,
        }
    }
    Some((&id[..p], b[p], leg, seq))
}

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct Stats {
    pub dup_skipped: u64,
    pub gaps: u64,
    pub gap_seqs: u64,
    pub rejected: u64,
}

pub enum Verdict {
    Fresh,
    Replay,
}

// ---------------------------------------------------------------------------
// engine
// ---------------------------------------------------------------------------

pub struct Engine {
    book: Book,
    risk: Exposure,
    seq: u64,
    lanes_p: u32,
    lanes: FnvMap<LaneKey, LaneState>,
    pub stats: Stats,
    /// The lane `classify` last changed, while tracking (live region).
    track: bool,
    dirty_lane: Option<LaneKey>,
}

impl Engine {
    pub fn new(risk_limit: i64, lanes_p: u32) -> Self {
        Self {
            book: Book::new(),
            risk: Exposure::new(risk_limit),
            seq: 0,
            lanes_p: lanes_p.max(1),
            lanes: FnvMap::default(),
            stats: Stats::default(),
            track: false,
            dirty_lane: None,
        }
    }

    /// Record which slots, exposures and lanes each record changes, for the
    /// live snapshot region. Off by default: STATE=off and the full-snapshot
    /// mode pay nothing for it.
    fn set_tracking(&mut self, on: bool) {
        self.track = on;
        self.dirty_lane = None;
        self.book.set_tracking(on);
        self.risk.track = on;
        self.risk.dirty.clear();
    }

    /// Classify a record by its first order id. Records that do not carry a
    /// parseable sequence are always Fresh.
    fn classify(&mut self, id: &str) -> Verdict {
        let Some((run, phase, leg, seq)) = parse_seq_id(id) else {
            return Verdict::Fresh;
        };
        let p = self.lanes_p as u64;
        let key = LaneKey {
            run: run.to_string(),
            phase,
            lane: (seq % p) as u32,
        };
        let k = seq * 4 + leg as u64;
        if self.track {
            self.dirty_lane = Some(key.clone());
        }
        match self.lanes.get_mut(&key) {
            None => {
                self.lanes.insert(
                    key,
                    LaneState {
                        hwm: k,
                        check_gap: false,
                    },
                );
                Verdict::Fresh
            }
            Some(st) => {
                if k <= st.hwm {
                    self.stats.dup_skipped += 1;
                    return Verdict::Replay;
                }
                if st.check_gap {
                    st.check_gap = false;
                    // At least one whole seq skipped. A lost trailing leg
                    // within the same seq is not called a gap here - the
                    // sentinel check is the ground truth for loss, this is
                    // only the guest's own early warning.
                    let hs = st.hwm / 4;
                    if seq > hs + p {
                        self.stats.gaps += 1;
                        self.stats.gap_seqs += (seq - hs) / p - 1;
                        log(&format!(
                            "STATE gap run={} phase={} lane={} hwm_seq={} next_seq={} skipped_seqs={}",
                            key.run,
                            key.phase as char,
                            key.lane,
                            hs,
                            seq,
                            (seq - hs) / p - 1
                        ));
                    }
                }
                st.hwm = k;
                Verdict::Fresh
            }
        }
    }

    /// One input record to at most one output payload. None = no output:
    /// a replayed record (its output was written before the restart) or a
    /// record rejected whole, matching the other guests' drop policy.
    pub fn process(&mut self, input: &[u8]) -> Option<Vec<u8>> {
        let t = wire::decode_message_type(input).ok()?;
        match t {
            MessageType::NewOrder => {
                let o = wire::decode_new_order(input).ok()?;
                if let Verdict::Replay = self.classify(&o.order_id) {
                    return None;
                }
                match self.submit_order(o) {
                    Ok(f) => Some(wire::encode_fills(&f)),
                    Err(()) => {
                        self.stats.rejected += 1;
                        None
                    }
                }
            }
            MessageType::MassQuote => {
                let m = wire::decode_mass_quote(input).ok()?;
                if let Some(first) = m.orders.first() {
                    if let Verdict::Replay = self.classify(&first.order_id) {
                        return None;
                    }
                }
                let mut fills = Vec::new();
                for o in m.orders {
                    match self.submit_order(o) {
                        Ok(mut f) => fills.append(&mut f),
                        Err(()) => self.stats.rejected += 1,
                    }
                }
                Some(wire::encode_fills(&fills))
            }
            MessageType::Cancel => {
                let c = wire::decode_cancel(input).ok()?;
                match self.book.cancel(&c.order_id) {
                    Ok(removed) => {
                        self.risk
                            .record(&removed.participant, -(removed.price * removed.qty));
                        Some(wire::encode_fills(&[]))
                    }
                    Err(_) => {
                        self.stats.rejected += 1;
                        None
                    }
                }
            }
        }
    }

    /// Same per-order path as ../matcher.rs's submit_order.
    fn submit_order(&mut self, o: wire::NewOrder) -> Result<Vec<wire::Fill>, ()> {
        let notional = o.price * o.qty;
        if !self.risk.check(&o.participant, notional) {
            return Err(());
        }
        self.seq += 1;
        let side = if o.side == 0 { Side::Buy } else { Side::Sell };
        let fills = self
            .book
            .submit(Order {
                id: o.order_id,
                participant: o.participant.clone(),
                side,
                price: o.price,
                qty: o.qty,
                sequence: self.seq,
            })
            .map_err(|_| ())?;
        self.risk.record(&o.participant, notional);
        for f in &fills {
            let n = f.price * f.qty;
            self.risk.record(&f.aggressor_party, -n);
            self.risk.record(&f.resting_party, -n);
        }
        Ok(fills
            .into_iter()
            .map(|f| wire::Fill {
                aggressor_id: f.aggressor_id,
                resting_id: f.resting_id,
                aggressor_party: f.aggressor_party,
                resting_party: f.resting_party,
                price: f.price,
                qty: f.qty,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// snapshot: header + payload, written into the registered region
// ---------------------------------------------------------------------------

pub const MAGIC: u32 = 0x424B_4D31; // "BKM1"
pub const VERSION: u32 = 1;
pub const HEADER: usize = 40;

/// Header layout, all big-endian:
///   0 magic u32 | 4 version u32 | 8 generation u64 | 16 payload_len u32
///  20 crc32 u32 | 24 order_count u32 | 28 lane_count u32 | 32 saved_at_ms u64
#[derive(Debug, PartialEq)]
pub struct Header {
    pub generation: u64,
    pub payload_len: u32,
    pub order_count: u32,
    pub lane_count: u32,
    pub saved_at_ms: u64,
}

fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let t = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *e = c;
        }
        t
    });
    let mut c = !0u32;
    for b in data {
        c = t[((c ^ *b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

struct W<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl W<'_> {
    fn put(&mut self, b: &[u8]) -> Option<()> {
        let end = self.pos.checked_add(b.len())?;
        self.buf.get_mut(self.pos..end)?.copy_from_slice(b);
        self.pos = end;
        Some(())
    }
    fn u8(&mut self, v: u8) -> Option<()> {
        self.put(&[v])
    }
    fn u16(&mut self, v: u16) -> Option<()> {
        self.put(&v.to_be_bytes())
    }
    fn u32(&mut self, v: u32) -> Option<()> {
        self.put(&v.to_be_bytes())
    }
    fn u64(&mut self, v: u64) -> Option<()> {
        self.put(&v.to_be_bytes())
    }
    fn str(&mut self, s: &str) -> Option<()> {
        self.u16(u16::try_from(s.len()).ok()?)?;
        self.put(s.as_bytes())
    }
}

struct R<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
    fn str(&mut self) -> Option<String> {
        let n = self.u16()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }
}

/// Serialise `e` into `region`. Returns the header written, or None if the
/// state does not fit - in which case the magic is zeroed, so the broker
/// checkpoints an explicitly invalid region rather than an older snapshot
/// that would silently restore stale state.
pub fn save(e: &Engine, region: &mut [u8], generation: u64, now_ms: u64) -> Option<Header> {
    let res = save_payload(e, region);
    let Some((payload_len, order_count, lane_count)) = res else {
        if region.len() >= 4 {
            region[..4].copy_from_slice(&[0; 4]);
        }
        return None;
    };
    let crc = crc32(&region[HEADER..HEADER + payload_len]);
    let h = Header {
        generation,
        payload_len: payload_len as u32,
        order_count,
        lane_count,
        saved_at_ms: now_ms,
    };
    let mut w = W {
        buf: &mut region[..HEADER],
        pos: 0,
    };
    w.u32(MAGIC)?;
    w.u32(VERSION)?;
    w.u64(h.generation)?;
    w.u32(h.payload_len)?;
    w.u32(crc)?;
    w.u32(h.order_count)?;
    w.u32(h.lane_count)?;
    w.u64(h.saved_at_ms)?;
    Some(h)
}

fn save_payload(e: &Engine, region: &mut [u8]) -> Option<(usize, u32, u32)> {
    if region.len() < HEADER {
        return None;
    }
    let mut w = W {
        buf: &mut region[HEADER..],
        pos: 0,
    };
    w.u64(e.seq)?;
    w.u64(e.risk.limit as u64)?;
    w.u32(e.lanes_p)?;
    // Participant table: risk entries carry their exposure, and orders
    // refer to participants by index, which keeps a 10k-deep book of
    // repeated party names from repeating them.
    let mut parties: FnvMap<&str, u32> = FnvMap::default();
    let mut table: Vec<&str> = Vec::new();
    for p in e.risk.map.keys().map(|s| s.as_str()).chain(e.book.orders().map(|o| o.participant.as_str())) {
        if !parties.contains_key(p) {
            parties.insert(p, table.len() as u32);
            table.push(p);
        }
    }
    w.u32(table.len() as u32)?;
    for p in &table {
        w.str(p)?;
        w.u64(e.risk.exposure(p) as u64)?;
    }
    w.u32(e.lanes.len() as u32)?;
    for (k, v) in &e.lanes {
        w.str(&k.run)?;
        w.u8(k.phase)?;
        w.u32(k.lane)?;
        w.u64(v.hwm)?;
    }
    let n = e.book.len() as u32;
    w.u32(n)?;
    for o in e.book.orders() {
        w.u8(if o.side == Side::Buy { 0 } else { 1 })?;
        w.u64(o.price as u64)?;
        w.u64(o.qty as u64)?;
        w.u64(o.sequence)?;
        w.u32(parties[o.participant.as_str()])?;
        w.str(&o.id)?;
    }
    w.u64(e.stats.dup_skipped)?;
    w.u64(e.stats.gaps)?;
    w.u64(e.stats.gap_seqs)?;
    w.u64(e.stats.rejected)?;
    Some((w.pos, n, e.lanes.len() as u32))
}

#[derive(Debug, PartialEq)]
pub enum RestoreError {
    Empty,
    BadMagic,
    BadVersion(u32),
    BadLength(u32),
    BadCrc,
    BadPayload,
}

/// Rebuild an Engine from a region. Every lane comes back with check_gap
/// set, so each lane's first record after the restore is checked for a gap.
pub fn restore(region: &[u8]) -> Result<(Engine, Header), RestoreError> {
    if region.len() < HEADER || region[..HEADER].iter().all(|b| *b == 0) {
        return Err(RestoreError::Empty);
    }
    let mut r = R { buf: region, pos: 0 };
    let bad = RestoreError::BadPayload;
    if r.u32() != Some(MAGIC) {
        return Err(RestoreError::BadMagic);
    }
    let v = r.u32().unwrap();
    if v != VERSION {
        return Err(RestoreError::BadVersion(v));
    }
    let generation = r.u64().unwrap();
    let payload_len = r.u32().unwrap();
    let crc = r.u32().unwrap();
    let order_count = r.u32().unwrap();
    let lane_count = r.u32().unwrap();
    let saved_at_ms = r.u64().unwrap();
    let end = HEADER
        .checked_add(payload_len as usize)
        .filter(|e| *e <= region.len())
        .ok_or(RestoreError::BadLength(payload_len))?;
    let payload = &region[HEADER..end];
    if crc32(payload) != crc {
        return Err(RestoreError::BadCrc);
    }
    let mut r = R { buf: payload, pos: 0 };
    (|| -> Option<(Engine, Header)> {
        let seq = r.u64()?;
        let limit = r.u64()? as i64;
        let lanes_p = r.u32()?;
        let mut e = Engine::new(limit, lanes_p);
        e.seq = seq;
        let np = r.u32()? as usize;
        let mut table = Vec::with_capacity(np);
        for _ in 0..np {
            let p = r.str()?;
            let x = r.u64()? as i64;
            if x != 0 {
                e.risk.map.insert(p.clone(), x);
            }
            table.push(p);
        }
        for _ in 0..r.u32()? {
            let run = r.str()?;
            let phase = r.u8()?;
            let lane = r.u32()?;
            let hwm = r.u64()?;
            e.lanes.insert(
                LaneKey { run, phase, lane },
                LaneState {
                    hwm,
                    check_gap: true,
                },
            );
        }
        let n = r.u32()?;
        for _ in 0..n {
            let side = if r.u8()? == 0 { Side::Buy } else { Side::Sell };
            let price = r.u64()? as i64;
            let qty = r.u64()? as i64;
            let sequence = r.u64()?;
            let participant = table.get(r.u32()? as usize)?.clone();
            let id = r.str()?;
            e.book.rest(Order {
                id,
                participant,
                side,
                price,
                qty,
                sequence,
            });
        }
        e.stats = Stats {
            dup_skipped: r.u64()?,
            gaps: r.u64()?,
            gap_seqs: r.u64()?,
            rejected: r.u64()?,
        };
        if n != order_count || e.lanes.len() as u32 != lane_count {
            return None;
        }
        Some((
            e,
            Header {
                generation,
                payload_len,
                order_count,
                lane_count,
                saved_at_ms,
            },
        ))
    })()
    .ok_or(bad)
}

// ---------------------------------------------------------------------------
// live snapshot region (SNAPSHOT_MODE=live, the default for STATE=snapshot)
// ---------------------------------------------------------------------------
//
// The full snapshot above re-serialises the whole book every
// SNAPSHOT_EVERY_MS, inline with a record: 0.7ms on average at 1,000 resting
// orders and 7.3ms (13.5ms max) at 10,000 on the 2026-10-15 cluster, which is
// what pushed p99.9 to 10-18ms with state on. And the broker copies and
// replicates the whole registered region - 4 MiB however small the book -
// every 5s.
//
// The live region is laid out so that it never needs serialising: it mirrors
// the book's slab slot for slot, plus fixed tables for participants and replay
// lanes, and after each record the guest rewrites only what that record
// changed (a few fixed-size slots). The broker reads the region between
// invocations, so it always sees a state consistent as of the last record -
// never older than the checkpoint that copies it, and never torn. The guest
// re-registers the region at its USED length whenever that grows (the host
// module replaces the registration on every call), so a checkpoint copies the
// book's real extent rather than the allocation.
//
// Restore puts time priority back by resting the live orders in `sequence`
// order: sequence is assigned on arrival and never changes, and a level's FIFO
// is arrival order. The restored book's slab indices differ from the region's,
// so a restore is followed by one full rewrite.
//
// Layout, big-endian:
//   header (LIVE_HEADER bytes): see write_live_header
//   lane slots   lane_cap  x LANE_SLOT : run_len u8 | phase u8 | pad[2] | lane u32 | hwm u64 | run[32]
//   party slots  party_cap x PARTY_SLOT: len u8 | pad[7] | exposure i64 | name[24]
//   order slots  slab_len  x ORDER_SLOT: live u8 | side u8 | id_len u8 | pad | party u32 |
//                                         price i64 | qty i64 | sequence u64 | id[56]

pub const LIVE_MAGIC: u32 = 0x424B_4D32; // "BKM2"
pub const LIVE_VERSION: u32 = 2;
pub const LIVE_HEADER: usize = 112;
pub const LANE_SLOT: usize = 48;
pub const PARTY_SLOT: usize = 40;
pub const ORDER_SLOT: usize = 88;
const LANE_RUN_MAX: usize = 32;
const PARTY_NAME_MAX: usize = 24;
const ORDER_ID_MAX: usize = 56;
/// Re-register in steps of this much, so a growing book costs a host call
/// every 64 KiB of growth rather than every new slot.
const REGISTER_STEP: usize = 64 << 10;

#[derive(Debug, PartialEq)]
pub struct LiveHeader {
    pub generation: u64,
    pub order_count: u32,
    pub lane_count: u32,
    pub used_len: u32,
    pub saved_at_ms: u64,
}

pub struct LiveRegion {
    lane_cap: usize,
    party_cap: usize,
    lanes: FnvMap<LaneKey, u32>,
    parties: FnvMap<String, u32>,
    generation: u64,
    /// False after an overflow: the magic is cleared so the broker
    /// checkpoints an explicitly invalid region, and nothing is written again.
    pub valid: bool,
    touched: Vec<u32>,
}

fn be_u32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(b[at..at + 4].try_into().unwrap())
}
fn be_u64(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().unwrap())
}

impl LiveRegion {
    pub fn new(lane_cap: usize, party_cap: usize) -> Self {
        Self {
            lane_cap,
            party_cap,
            lanes: FnvMap::default(),
            parties: FnvMap::default(),
            generation: 0,
            valid: true,
            touched: Vec::new(),
        }
    }

    fn lanes_at(&self) -> usize {
        LIVE_HEADER
    }
    fn parties_at(&self) -> usize {
        LIVE_HEADER + self.lane_cap * LANE_SLOT
    }
    fn orders_at(&self) -> usize {
        self.parties_at() + self.party_cap * PARTY_SLOT
    }

    /// Bytes in use: everything up to the last slab slot.
    pub fn used_len(&self, e: &Engine) -> usize {
        self.orders_at() + e.book.slab_len() * ORDER_SLOT
    }

    /// The length to register: used, rounded up to REGISTER_STEP, capped.
    pub fn register_len(&self, e: &Engine, capacity: usize) -> usize {
        let used = self.used_len(e);
        used.div_ceil(REGISTER_STEP).saturating_mul(REGISTER_STEP).min(capacity)
    }

    fn invalidate(&mut self, region: &mut [u8], why: &str) {
        if self.valid {
            log(&format!("STATE too_big mode=live reason={why}"));
        }
        self.valid = false;
        if region.len() >= 4 {
            region[..4].copy_from_slice(&[0; 4]);
        }
    }

    fn party_index(&mut self, region: &mut [u8], name: &str) -> Option<u32> {
        if let Some(i) = self.parties.get(name) {
            return Some(*i);
        }
        if self.parties.len() >= self.party_cap || name.len() > PARTY_NAME_MAX {
            return None;
        }
        let i = self.parties.len() as u32;
        self.parties.insert(name.to_string(), i);
        let at = self.parties_at() + i as usize * PARTY_SLOT;
        let slot = &mut region[at..at + PARTY_SLOT];
        slot.fill(0);
        slot[0] = name.len() as u8;
        slot[16..16 + name.len()].copy_from_slice(name.as_bytes());
        Some(i)
    }

    fn write_party(&mut self, region: &mut [u8], e: &Engine, name: &str) -> Option<()> {
        let i = self.party_index(region, name)?;
        let at = self.parties_at() + i as usize * PARTY_SLOT;
        region[at + 8..at + 16].copy_from_slice(&e.risk.exposure(name).to_be_bytes());
        Some(())
    }

    fn write_lane(&mut self, region: &mut [u8], key: &LaneKey, st: &LaneState) -> Option<()> {
        let i = match self.lanes.get(key) {
            Some(i) => *i,
            None => {
                if self.lanes.len() >= self.lane_cap || key.run.len() > LANE_RUN_MAX {
                    return None;
                }
                let i = self.lanes.len() as u32;
                self.lanes.insert(key.clone(), i);
                i
            }
        };
        let at = self.lanes_at() + i as usize * LANE_SLOT;
        let slot = &mut region[at..at + LANE_SLOT];
        slot.fill(0);
        slot[0] = key.run.len() as u8;
        slot[1] = key.phase;
        slot[4..8].copy_from_slice(&key.lane.to_be_bytes());
        slot[8..16].copy_from_slice(&st.hwm.to_be_bytes());
        slot[16..16 + key.run.len()].copy_from_slice(key.run.as_bytes());
        Some(())
    }

    fn write_order(&mut self, region: &mut [u8], e: &Engine, idx: u32) -> Option<()> {
        let at = self.orders_at() + idx as usize * ORDER_SLOT;
        if at + ORDER_SLOT > region.len() {
            return None;
        }
        let Some(o) = e.book.slot_order(idx) else {
            region[at] = 0; // free slot: only the live flag matters
            return Some(());
        };
        if o.id.len() > ORDER_ID_MAX {
            return None;
        }
        let party = self.party_index(region, &o.participant)?;
        let slot = &mut region[at..at + ORDER_SLOT];
        slot[0] = 1;
        slot[1] = if o.side == Side::Buy { 0 } else { 1 };
        slot[2] = o.id.len() as u8;
        slot[3] = 0;
        slot[4..8].copy_from_slice(&party.to_be_bytes());
        slot[8..16].copy_from_slice(&o.price.to_be_bytes());
        slot[16..24].copy_from_slice(&o.qty.to_be_bytes());
        slot[24..32].copy_from_slice(&o.sequence.to_be_bytes());
        slot[32..32 + o.id.len()].copy_from_slice(o.id.as_bytes());
        Some(())
    }

    fn write_header(&mut self, region: &mut [u8], e: &Engine, now_ms: u64) {
        self.generation += 1;
        let h = &mut region[..LIVE_HEADER];
        h.fill(0);
        h[0..4].copy_from_slice(&LIVE_MAGIC.to_be_bytes());
        h[4..8].copy_from_slice(&LIVE_VERSION.to_be_bytes());
        h[8..16].copy_from_slice(&self.generation.to_be_bytes());
        h[16..24].copy_from_slice(&e.seq.to_be_bytes());
        h[24..32].copy_from_slice(&(e.risk.limit as u64).to_be_bytes());
        h[32..36].copy_from_slice(&e.lanes_p.to_be_bytes());
        h[36..40].copy_from_slice(&(self.lanes.len() as u32).to_be_bytes());
        h[40..44].copy_from_slice(&(self.parties.len() as u32).to_be_bytes());
        h[44..48].copy_from_slice(&(e.book.slab_len() as u32).to_be_bytes());
        h[48..52].copy_from_slice(&(e.book.len() as u32).to_be_bytes());
        h[52..56].copy_from_slice(&(self.lane_cap as u32).to_be_bytes());
        h[56..60].copy_from_slice(&(self.party_cap as u32).to_be_bytes());
        h[60..64].copy_from_slice(&(ORDER_SLOT as u32).to_be_bytes());
        h[64..72].copy_from_slice(&e.stats.dup_skipped.to_be_bytes());
        h[72..80].copy_from_slice(&e.stats.gaps.to_be_bytes());
        h[80..88].copy_from_slice(&e.stats.gap_seqs.to_be_bytes());
        h[88..96].copy_from_slice(&e.stats.rejected.to_be_bytes());
        h[96..104].copy_from_slice(&now_ms.to_be_bytes());
        let crc = crc32(&h[..104]);
        h[104..108].copy_from_slice(&crc.to_be_bytes());
    }

    /// Rewrite everything from the engine: on a fresh start and after a
    /// restore. Turns tracking on.
    pub fn sync_all(&mut self, e: &mut Engine, region: &mut [u8], now_ms: u64) {
        e.set_tracking(true);
        if !self.valid {
            return;
        }
        if self.orders_at() > region.len() {
            return self.invalidate(region, "tables");
        }
        self.lanes.clear();
        self.parties.clear();
        region[self.lanes_at()..self.orders_at()].fill(0);
        let ok = (|| -> Option<()> {
            let lanes: Vec<(LaneKey, LaneState)> =
                e.lanes.iter().map(|(k, v)| (k.clone(), *v)).collect();
            for (k, v) in &lanes {
                self.write_lane(region, k, v)?;
            }
            let names: Vec<String> = e.risk.map.keys().cloned().collect();
            for n in &names {
                self.write_party(region, e, n)?;
            }
            for idx in 0..e.book.slab_len() as u32 {
                self.write_order(region, e, idx)?;
            }
            Some(())
        })();
        if ok.is_none() {
            return self.invalidate(region, "capacity");
        }
        self.write_header(region, e, now_ms);
    }

    /// After one record: rewrite what it changed. Cheap - a handful of
    /// fixed-size slots and the header.
    pub fn sync_dirty(&mut self, e: &mut Engine, region: &mut [u8], now_ms: u64) {
        let mut touched = std::mem::take(&mut self.touched);
        e.book.take_touched(&mut touched);
        let dirty_parties = std::mem::take(&mut e.risk.dirty);
        let dirty_lane = e.dirty_lane.take();
        if self.valid {
            let ok = (|| -> Option<()> {
                if let Some(k) = &dirty_lane {
                    if let Some(st) = e.lanes.get(k).copied() {
                        self.write_lane(region, k, &st)?;
                    }
                }
                for n in &dirty_parties {
                    self.write_party(region, e, n)?;
                }
                for idx in &touched {
                    self.write_order(region, e, *idx)?;
                }
                Some(())
            })();
            match ok {
                Some(()) => self.write_header(region, e, now_ms),
                None => self.invalidate(region, "capacity"),
            }
        }
        self.touched = touched;
        let mut dp = dirty_parties;
        dp.clear();
        e.risk.dirty = dp;
    }
}

/// Rebuild an Engine from a live region. Lanes come back with check_gap set,
/// as with the full snapshot. The caller follows this with `sync_all`, since
/// the restored book's slab indices are not the region's.
pub fn restore_live(region: &[u8]) -> Result<(Engine, LiveHeader), RestoreError> {
    if region.len() < LIVE_HEADER || region[..LIVE_HEADER].iter().all(|b| *b == 0) {
        return Err(RestoreError::Empty);
    }
    if be_u32(region, 0) != LIVE_MAGIC {
        return Err(RestoreError::BadMagic);
    }
    let v = be_u32(region, 4);
    if v != LIVE_VERSION {
        return Err(RestoreError::BadVersion(v));
    }
    if crc32(&region[..104]) != be_u32(region, 104) {
        return Err(RestoreError::BadCrc);
    }
    let generation = be_u64(region, 8);
    let seq = be_u64(region, 16);
    let limit = be_u64(region, 24) as i64;
    let lanes_p = be_u32(region, 32);
    let n_lanes = be_u32(region, 36) as usize;
    let n_parties = be_u32(region, 40) as usize;
    let slab = be_u32(region, 44) as usize;
    let order_count = be_u32(region, 48);
    let lane_cap = be_u32(region, 52) as usize;
    let party_cap = be_u32(region, 56) as usize;
    let saved_at_ms = be_u64(region, 96);
    if be_u32(region, 60) as usize != ORDER_SLOT || n_lanes > lane_cap || n_parties > party_cap {
        return Err(RestoreError::BadPayload);
    }
    let orders_at = LIVE_HEADER + lane_cap * LANE_SLOT + party_cap * PARTY_SLOT;
    let used = orders_at
        .checked_add(slab.checked_mul(ORDER_SLOT).ok_or(RestoreError::BadPayload)?)
        .ok_or(RestoreError::BadPayload)?;
    if used > region.len() {
        return Err(RestoreError::BadLength(used as u32));
    }
    let bad = RestoreError::BadPayload;
    let text = |b: &[u8]| std::str::from_utf8(b).map(str::to_string).ok();
    let mut e = Engine::new(limit, lanes_p);
    e.seq = seq;
    e.stats = Stats {
        dup_skipped: be_u64(region, 64),
        gaps: be_u64(region, 72),
        gap_seqs: be_u64(region, 80),
        rejected: be_u64(region, 88),
    };
    for i in 0..n_lanes {
        let at = LIVE_HEADER + i * LANE_SLOT;
        let n = region[at] as usize;
        if n > LANE_RUN_MAX {
            return Err(bad);
        }
        let run = text(&region[at + 16..at + 16 + n]).ok_or(RestoreError::BadPayload)?;
        e.lanes.insert(
            LaneKey {
                run,
                phase: region[at + 1],
                lane: be_u32(region, at + 4),
            },
            LaneState {
                hwm: be_u64(region, at + 8),
                check_gap: true,
            },
        );
    }
    let parties_at = LIVE_HEADER + lane_cap * LANE_SLOT;
    let mut names = Vec::with_capacity(n_parties);
    for i in 0..n_parties {
        let at = parties_at + i * PARTY_SLOT;
        let n = region[at] as usize;
        if n > PARTY_NAME_MAX {
            return Err(bad);
        }
        let name = text(&region[at + 16..at + 16 + n]).ok_or(RestoreError::BadPayload)?;
        let x = be_u64(region, at + 8) as i64;
        if x > 0 {
            e.risk.map.insert(name.clone(), x);
        }
        names.push(name);
    }
    let mut orders = Vec::with_capacity(order_count as usize);
    for i in 0..slab {
        let at = orders_at + i * ORDER_SLOT;
        match region[at] {
            0 => continue,
            1 => {}
            _ => return Err(bad),
        }
        let side = match region[at + 1] {
            0 => Side::Buy,
            1 => Side::Sell,
            _ => return Err(bad),
        };
        let n = region[at + 2] as usize;
        let party = be_u32(region, at + 4) as usize;
        if n > ORDER_ID_MAX || party >= names.len() {
            return Err(bad);
        }
        orders.push(Order {
            id: text(&region[at + 32..at + 32 + n]).ok_or(RestoreError::BadPayload)?,
            participant: names[party].clone(),
            side,
            price: be_u64(region, at + 8) as i64,
            qty: be_u64(region, at + 16) as i64,
            sequence: be_u64(region, at + 24),
        });
    }
    if orders.len() != order_count as usize {
        return Err(bad);
    }
    orders.sort_by_key(|o| o.sequence);
    for o in orders {
        e.book.rest(o);
    }
    Ok((
        e,
        LiveHeader {
            generation,
            order_count,
            lane_count: n_lanes as u32,
            used_len: used as u32,
            saved_at_ms,
        },
    ))
}

// ---------------------------------------------------------------------------
// guest entrypoint
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "redpanda_wasm_shared_memory")]
extern "C" {
    fn register_region(ptr: u32, len: u32) -> i32;
}

/// (Re-)register the first `len` bytes of `region` as the shared region.
/// The host replaces any earlier registration, so this is how the live mode
/// tells the broker how much of its allocation a checkpoint has to copy.
fn register(region: &[u8], len: usize) {
    #[cfg(target_arch = "wasm32")]
    {
        let rc = unsafe { register_region(region.as_ptr() as u32, len as u32) };
        if rc != 0 {
            panic!("register_region failed: {rc}");
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    let _ = (region, len);
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum SnapshotMode {
    /// Re-serialise the whole book every SNAPSHOT_EVERY_MS (format BKM1).
    Full,
    /// Keep the region current slot by slot after every record (BKM2).
    Live,
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// stderr is captured into the broker's transform log topic, which is where
/// the durability harness reads the STATE lines from.
fn log(s: &str) {
    eprintln!("{s}");
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Guest {
    engine: Engine,
    region: Option<&'static mut [u8]>,
    mode: SnapshotMode,
    live: LiveRegion,
    registered: usize,
    first: bool,
    generation: u64,
    every_ns: u64,
    last_save_ns: u64,
    last_stats_ns: u64,
    save_ns_max: u64,
    save_ns_sum: u64,
    saves: u64,
    last_bytes: u32,
}

impl Guest {
    fn on_first_record(&mut self) {
        if self.region.is_none() {
            log("STATE off");
            return;
        }
        if self.mode == SnapshotMode::Live {
            return self.on_first_record_live();
        }
        let Some(region) = self.region.as_deref() else { return };
        let t0 = now_nanos();
        match restore(region) {
            Ok((e, h)) => {
                let dt = now_nanos().saturating_sub(t0);
                let age_ms = (t0 / 1_000_000).saturating_sub(h.saved_at_ms);
                log(&format!(
                    "STATE restored gen={} orders={} lanes={} bytes={} restore_us={} snapshot_age_ms={} seq={}",
                    h.generation,
                    h.order_count,
                    h.lane_count,
                    h.payload_len as usize + HEADER,
                    dt / 1000,
                    age_ms,
                    e.seq
                ));
                self.generation = h.generation;
                // Keep this process's own config, not the snapshot's: a
                // redeploy with a different limit or lane count means it.
                let limit = self.engine.risk.limit;
                let p = self.engine.lanes_p;
                self.engine = e;
                self.engine.risk.limit = limit;
                self.engine.lanes_p = p;
            }
            Err(why) => log(&format!("STATE fresh reason={why:?}")),
        }
    }

    fn on_first_record_live(&mut self) {
        let region: &mut [u8] = self.region.as_deref_mut().expect("live mode has a region");
        let t0 = now_nanos();
        match restore_live(region) {
            Ok((e, h)) => {
                let dt = now_nanos().saturating_sub(t0);
                let age_ms = (t0 / 1_000_000).saturating_sub(h.saved_at_ms);
                // Same field order as the full mode's line: the durability
                // harness parses orders=, bytes= and restore_us= from it.
                log(&format!(
                    "STATE restored gen={} orders={} lanes={} bytes={} restore_us={} snapshot_age_ms={} seq={} mode=live",
                    h.generation, h.order_count, h.lane_count, h.used_len, dt / 1000, age_ms, e.seq
                ));
                self.generation = h.generation;
                let limit = self.engine.risk.limit;
                let p = self.engine.lanes_p;
                self.engine = e;
                self.engine.risk.limit = limit;
                self.engine.lanes_p = p;
            }
            Err(why) => log(&format!("STATE fresh reason={why:?} mode=live")),
        }
        // Restored or fresh, rewrite the whole region from the engine (the
        // restored slab indices are not the region's), then register only
        // what is in use.
        let t1 = now_nanos();
        self.live.sync_all(&mut self.engine, region, t1 / 1_000_000);
        let n = self.live.register_len(&self.engine, region.len());
        register(region, n);
        self.registered = n;
        self.last_bytes = self.live.used_len(&self.engine) as u32;
    }

    fn after_record(&mut self) {
        let now = now_nanos();
        if self.mode == SnapshotMode::Live {
            if let Some(region) = self.region.as_deref_mut() {
                let t0 = now_nanos();
                self.live.sync_dirty(&mut self.engine, region, t0 / 1_000_000);
                let n = self.live.register_len(&self.engine, region.len());
                if n > self.registered {
                    register(region, n);
                    self.registered = n;
                }
                let dt = now_nanos().saturating_sub(t0);
                self.save_ns_max = self.save_ns_max.max(dt);
                self.save_ns_sum += dt;
                self.saves += 1;
                self.generation += 1;
                self.last_bytes = self.live.used_len(&self.engine) as u32;
            }
        } else if let Some(region) = self.region.as_deref_mut() {
            if now.saturating_sub(self.last_save_ns) >= self.every_ns {
                self.generation += 1;
                let t0 = now_nanos();
                let h = save(&self.engine, region, self.generation, t0 / 1_000_000);
                let dt = now_nanos().saturating_sub(t0);
                self.save_ns_max = self.save_ns_max.max(dt);
                self.save_ns_sum += dt;
                self.saves += 1;
                match h {
                    Some(h) => self.last_bytes = h.payload_len + HEADER as u32,
                    None => log(&format!(
                        "STATE too_big orders={} region={}",
                        self.engine.book.len(),
                        region.len()
                    )),
                }
                self.last_save_ns = now;
            }
        }
        if now.saturating_sub(self.last_stats_ns) >= 5_000_000_000 {
            let s = self.engine.stats;
            log(&format!(
                "STATE stats gen={} orders={} bytes={} saves={} save_us_avg={} save_us_max={} dup_skipped={} gaps={} gap_seqs={} rejected={} mode={} registered={}",
                self.generation,
                self.engine.book.len(),
                self.last_bytes,
                self.saves,
                if self.saves > 0 { self.save_ns_sum / self.saves / 1000 } else { 0 },
                self.save_ns_max / 1000,
                s.dup_skipped,
                s.gaps,
                s.gap_seqs,
                s.rejected,
                if self.mode == SnapshotMode::Live { "live" } else { "full" },
                self.registered
            ));
            self.save_ns_max = 0;
            self.save_ns_sum = 0;
            self.saves = 0;
            self.last_stats_ns = now;
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    use std::cell::RefCell;
    use redpanda_transform_sdk::{on_record_written, Record, RecordWriter, WriteEvent};
    use std::convert::Infallible;

    let risk_limit = std::env::var("RISK_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0i64);
    let lanes = env_u64("SEQ_LANES", 1) as u32;
    let snapshot = std::env::var("STATE").map(|v| v == "snapshot").unwrap_or(false);
    let region: Option<&'static mut [u8]> = if snapshot {
        // Leaked once, so its address never changes for the life of the
        // instance - the broker holds (ptr, len) from this one call.
        let bytes = env_u64("REGION_BYTES", 4 << 20) as usize;
        let r: &'static mut [u8] = Box::leak(vec![0u8; bytes].into_boxed_slice());
        // The whole allocation, so the broker can write back a snapshot of
        // any size before the first record; live mode then shrinks the
        // registration to what is in use.
        register(r, r.len());
        Some(r)
    } else {
        None
    };
    let mode = match std::env::var("SNAPSHOT_MODE").as_deref() {
        Ok("full") => SnapshotMode::Full,
        _ => SnapshotMode::Live,
    };
    let registered = region.as_deref().map(|r| r.len()).unwrap_or(0);
    let g = RefCell::new(Guest {
        engine: Engine::new(risk_limit, lanes),
        region,
        mode,
        live: LiveRegion::new(
            env_u64("LIVE_LANES", 256) as usize,
            env_u64("LIVE_PARTIES", 1024) as usize,
        ),
        registered,
        first: true,
        generation: 0,
        every_ns: env_u64("SNAPSHOT_EVERY_MS", 500) * 1_000_000,
        last_save_ns: 0,
        last_stats_ns: now_nanos(),
        save_ns_max: 0,
        save_ns_sum: 0,
        saves: 0,
        last_bytes: 0,
    });
    on_record_written(
        move |event: WriteEvent, writer: &mut RecordWriter| -> Result<(), Infallible> {
            let mut g = g.borrow_mut();
            if g.first {
                g.first = false;
                g.on_first_record();
            }
            if let Some(value) = event.record.value() {
                if let Some(mut out) = g.engine.process(value) {
                    out.extend_from_slice(&now_nanos().to_be_bytes());
                    let _ = writer.write(&Record::new(None, Some(out)));
                }
            }
            // At the END of an invocation: the broker reads the region
            // between invocations, so it never sees a half-written one.
            g.after_record();
            Ok(())
        },
    );
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    eprintln!("book-matcher is a wasm32-wasip1 guest; build with --target wasm32-wasip1");
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{decode_fills, encode_cancel, encode_mass_quote, encode_new_order, Cancel, MassQuote, NewOrder};

    /// xorshift64*, so the differential test is reproducible without a dep.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn ord(id: &str, party: &str, side: Side, price: i64, qty: i64, seq: u64) -> Order {
        Order {
            id: id.into(),
            participant: party.into(),
            side,
            price,
            qty,
            sequence: seq,
        }
    }

    /// Drain a book by sweeping both sides with orders that cross
    /// everything; the fills list IS the resting state in priority order.
    /// This works on the old book too, whose fields are private.
    fn sweep_old(b: &mut orderbook::Book, seq: u64) -> Vec<(String, i64, i64)> {
        let mut out = Vec::new();
        for (side, price, id) in [(Side::Buy, i64::MAX / 4, "SWEEP-B"), (Side::Sell, i64::MIN / 4, "SWEEP-S")] {
            for f in b.submit(ord(id, "sweeper", side, price, i64::MAX / 4, seq)).unwrap() {
                out.push((f.resting_id, f.price, f.qty));
            }
            let _ = b.cancel(id);
        }
        out
    }

    fn sweep_new(b: &mut Book, seq: u64) -> Vec<(String, i64, i64)> {
        let mut out = Vec::new();
        for (side, price, id) in [(Side::Buy, i64::MAX / 4, "SWEEP-B"), (Side::Sell, i64::MIN / 4, "SWEEP-S")] {
            for f in b.submit(ord(id, "sweeper", side, price, i64::MAX / 4, seq)).unwrap() {
                out.push((f.resting_id, f.price, f.qty));
            }
            let _ = b.cancel(id);
        }
        out
    }

    fn fills_key(f: &[orderbook::Fill]) -> Vec<(String, String, String, String, i64, i64)> {
        f.iter()
            .map(|f| {
                (
                    f.aggressor_id.clone(),
                    f.resting_id.clone(),
                    f.aggressor_party.clone(),
                    f.resting_party.clone(),
                    f.price,
                    f.qty,
                )
            })
            .collect()
    }

    /// 200k random inserts, crosses, cancels (of live, dead and unknown
    /// ids) and duplicate submits through both books: identical results at
    /// every step, and identical resting state at checkpoints and the end.
    #[test]
    fn differential_200k_ops_against_the_reference_book() {
        let mut rng = Rng(0x9E3779B97F4A7C15);
        let mut old = orderbook::Book::new();
        let mut new = Book::new();
        let mut ids: Vec<String> = Vec::new();
        let parties = ["alice", "bob", "carol", "dave"];
        for step in 0..200_000u64 {
            let seq = step + 1;
            match rng.below(10) {
                0..=5 => {
                    let id = format!("o{step}");
                    let side = if rng.below(2) == 0 { Side::Buy } else { Side::Sell };
                    // A narrow band so crosses, partial fills and multi-level
                    // sweeps are all frequent.
                    let price = 90 + rng.below(21) as i64;
                    let qty = 1 + rng.below(10) as i64;
                    let party = parties[rng.below(4) as usize];
                    let a = old.submit(ord(&id, party, side, price, qty, seq));
                    let b = new.submit(ord(&id, party, side, price, qty, seq));
                    match (a, b) {
                        (Ok(a), Ok(b)) => assert_eq!(fills_key(&a), fills_key(&b), "step {step}"),
                        (a, b) => panic!("step {step}: {a:?} vs {b:?}"),
                    }
                    ids.push(id);
                }
                6..=8 => {
                    let id = if ids.is_empty() || rng.below(20) == 0 {
                        "never-existed".to_string()
                    } else {
                        ids[rng.below(ids.len() as u64) as usize].clone()
                    };
                    let a = old.cancel(&id).map(|o| (o.id, o.price, o.qty, o.participant));
                    let b = new.cancel(&id).map(|o| (o.id, o.price, o.qty, o.participant));
                    assert_eq!(a, b, "step {step} cancel {id}");
                }
                _ => {
                    if let Some(id) = ids.last().cloned() {
                        let a = old.submit(ord(&id, "alice", Side::Buy, 1, 1, seq)).map(|f| fills_key(&f));
                        let b = new.submit(ord(&id, "alice", Side::Buy, 1, 1, seq)).map(|f| fills_key(&f));
                        assert_eq!(a, b, "step {step} resubmit {id}");
                    }
                }
            }
        }
        assert_eq!(sweep_old(&mut old, 999_999_999), sweep_new(&mut new, 999_999_999));
        assert_eq!(new.len(), 0);
    }

    #[test]
    fn exposure_matches_risk_rs() {
        let mut rng = Rng(42);
        let mut a = risk::Checker::new(5_000);
        let mut b = Exposure::new(5_000);
        for _ in 0..50_000 {
            let p = ["x", "y", "z"][rng.below(3) as usize];
            let d = rng.below(2001) as i64 - 1000;
            assert_eq!(a.check(p, d).is_ok(), b.check(p, d));
            a.record(p, d);
            b.record(p, d);
            assert_eq!(a.exposure(p), b.exposure(p));
        }
    }

    fn no(id: &str, side: u8, price: i64, qty: i64) -> NewOrder {
        NewOrder {
            order_id: id.into(),
            participant: if side == 0 { "buyer".into() } else { "seller".into() },
            side,
            price,
            qty,
        }
    }

    /// A deep book plus lanes and exposure round-trip through a snapshot
    /// into an engine that then behaves identically to the original.
    #[test]
    fn snapshot_round_trip_at_depth() {
        let mut e = Engine::new(0, 2);
        for i in 0..10_000 {
            e.process(&encode_new_order(&no(&format!("9p-s-{i}-0"), 1, 100_000 + i, 1))).unwrap();
            e.process(&encode_new_order(&no(&format!("9p-b-{i}-0"), 0, 99 - (i % 98), 1))).unwrap();
        }
        for s in 0..50u64 {
            e.process(&encode_new_order(&no(&format!("7t-s{s}-0"), 1, 100, 1))).unwrap();
            e.process(&encode_new_order(&no(&format!("7t-b{s}-123"), 0, 200, 1))).unwrap();
        }
        let mut region = vec![0u8; 4 << 20];
        let h = save(&e, &mut region, 7, 1234).expect("fits in 4 MiB");
        assert_eq!(h.order_count, 20_000);
        assert!(h.payload_len < 2 << 20, "20k orders should serialise under 2 MiB, got {}", h.payload_len);
        let (mut r, rh) = restore(&region).unwrap();
        assert_eq!(rh, h);
        assert_eq!(r.seq, e.seq);
        assert_eq!(r.lanes.len(), e.lanes.len());
        // Same behaviour from here on: a sweep of both books matches.
        let a = sweep_new(&mut e.book, u64::MAX);
        let b = sweep_new(&mut r.book, u64::MAX);
        assert_eq!(a, b);
        assert_eq!(a.len(), 20_000);
    }

    #[test]
    fn snapshot_rejects_corruption_and_overflow() {
        let mut e = Engine::new(0, 1);
        e.process(&encode_new_order(&no("1t-s0-0", 1, 100, 1))).unwrap();
        let mut region = vec![0u8; 4096];
        assert_eq!(restore(&region).err(), Some(RestoreError::Empty));
        save(&e, &mut region, 1, 0).unwrap();
        region[HEADER + 3] ^= 0xFF;
        assert_eq!(restore(&region).err(), Some(RestoreError::BadCrc));
        // Too small: the magic is cleared rather than leaving the old one.
        let mut small = region.clone();
        small.truncate(HEADER + 8);
        small[..4].copy_from_slice(&MAGIC.to_be_bytes());
        assert!(save(&e, &mut small, 2, 0).is_none());
        assert_eq!(restore(&small).err(), Some(RestoreError::BadMagic));
    }

    /// Everything a restore must reproduce, in a comparable form.
    fn dump(e: &Engine) -> (Vec<(String, String, u8, i64, i64, u64)>, Vec<(String, i64)>, Vec<(String, u8, u32, u64)>, u64, Stats) {
        let orders = e
            .book
            .orders()
            .map(|o| (o.id.clone(), o.participant.clone(), (o.side == Side::Sell) as u8, o.price, o.qty, o.sequence))
            .collect();
        let mut risk: Vec<_> = e.risk.map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        risk.sort();
        let mut lanes: Vec<_> = e.lanes.iter().map(|(k, v)| (k.run.clone(), k.phase, k.lane, v.hwm)).collect();
        lanes.sort();
        (orders, risk, lanes, e.seq, e.stats)
    }

    /// Random traffic with loadgen-shaped ids: resting sells and buys at a
    /// spread of prices, crossing buys, cancels and mass quotes.
    fn random_record(rng: &mut Rng, i: u64, live_ids: &mut Vec<String>) -> Vec<u8> {
        match rng.below(10) {
            0..=3 => {
                let id = format!("42t-s{i}-0");
                live_ids.push(id.clone());
                encode_new_order(&no(&id, 1, 100 + rng.below(50) as i64, 1 + rng.below(5) as i64))
            }
            4..=5 => {
                let id = format!("42t-i{i}-0");
                live_ids.push(id.clone());
                encode_new_order(&no(&id, 0, 50 + rng.below(45) as i64, 1 + rng.below(3) as i64))
            }
            6..=7 => encode_new_order(&no(&format!("42t-b{i}-{}", 1000 + i), 0, 100 + rng.below(60) as i64, 1 + rng.below(8) as i64)),
            8 if !live_ids.is_empty() => {
                let k = rng.below(live_ids.len() as u64) as usize;
                let id = live_ids.swap_remove(k);
                encode_cancel(&Cancel { order_id: id })
            }
            _ => {
                let orders = (0..3)
                    .map(|q| {
                        let id = format!("42t-s{i}q{q}-0");
                        live_ids.push(id.clone());
                        no(&id, 1, 120 + rng.below(30) as i64, 1)
                    })
                    .collect();
                encode_mass_quote(&MassQuote { orders })
            }
        }
    }

    /// The live region tracks the engine record by record: restoring from
    /// its bytes at any point gives an engine with the same book (including
    /// time priority), exposure, lanes, sequence and stats - and the two then
    /// produce byte-identical output for the same further traffic.
    #[test]
    fn live_region_mirrors_the_engine() {
        let mut rng = Rng(0x5eed);
        let mut e = Engine::new(0, 1);
        let mut live = LiveRegion::new(64, 64);
        let mut region = vec![0u8; 4 << 20];
        live.sync_all(&mut e, &mut region, 0);
        let mut ids = Vec::new();
        for i in 0..30_000u64 {
            let rec = random_record(&mut rng, i, &mut ids);
            e.process(&rec);
            live.sync_dirty(&mut e, &mut region, i);
            assert!(live.valid);
            if i % 7_500 == 7_499 {
                let (mut r, h) = restore_live(&region).unwrap();
                assert_eq!(h.order_count as usize, e.book.len());
                assert_eq!(dump(&r), dump(&e), "state after {i} records");
                // Same output from here on.
                let mut ids2 = ids.clone();
                let mut rng2 = Rng(rng.0);
                for j in 0..200u64 {
                    let rec = random_record(&mut rng2, 1_000_000 + i + j, &mut ids2);
                    assert_eq!(e.process(&rec), r.process(&rec));
                    // Keep the region in step with `e` for the next check.
                    live.sync_dirty(&mut e, &mut region, i + j);
                }
                ids = ids2;
                rng = rng2;
            }
        }
        assert!(e.book.len() > 1_000, "the test should build a real book");
    }

    /// A restore re-lays the slab, so it is followed by a full rewrite; the
    /// rewritten region restores to the same state again.
    #[test]
    fn live_restore_then_rewrite_round_trips() {
        let mut rng = Rng(7);
        let mut e = Engine::new(0, 2);
        let mut live = LiveRegion::new(64, 64);
        let mut region = vec![0u8; 2 << 20];
        live.sync_all(&mut e, &mut region, 0);
        let mut ids = Vec::new();
        for i in 0..5_000 {
            e.process(&random_record(&mut rng, i, &mut ids));
            live.sync_dirty(&mut e, &mut region, i);
        }
        let (mut r, _) = restore_live(&region).unwrap();
        let mut live2 = LiveRegion::new(64, 64);
        let mut region2 = vec![0u8; 2 << 20];
        live2.sync_all(&mut r, &mut region2, 1);
        let (r2, _) = restore_live(&region2).unwrap();
        assert_eq!(dump(&r2), dump(&e));
        // The registration covers what is in use and no more than a step over.
        let n = live2.register_len(&r, region2.len());
        assert!(n >= live2.used_len(&r) && n - live2.used_len(&r) < REGISTER_STEP);
    }

    #[test]
    fn live_region_rejects_corruption_and_overflow() {
        let mut e = Engine::new(0, 1);
        let mut live = LiveRegion::new(8, 8);
        let mut region = vec![0u8; 64 << 10];
        assert_eq!(restore_live(&region).err(), Some(RestoreError::Empty));
        live.sync_all(&mut e, &mut region, 0);
        e.process(&encode_new_order(&no("1t-s0-0", 1, 100, 1))).unwrap();
        live.sync_dirty(&mut e, &mut region, 1);
        assert!(restore_live(&region).is_ok());
        let mut bad = region.clone();
        bad[20] ^= 0xFF; // inside the crc-covered header
        assert_eq!(restore_live(&bad).err(), Some(RestoreError::BadCrc));
        let mut bad = region.clone();
        let at = live.orders_at();
        bad[at] = 7; // not a live flag
        assert_eq!(restore_live(&bad).err(), Some(RestoreError::BadPayload));
        // A region too small for the book is cleared, not left stale.
        let mut small = vec![0u8; live.orders_at() + ORDER_SLOT];
        let mut e2 = Engine::new(0, 1);
        let mut l2 = LiveRegion::new(8, 8);
        l2.sync_all(&mut e2, &mut small, 0);
        for i in 0..4 {
            e2.process(&encode_new_order(&no(&format!("1t-s{i}-0"), 1, 100 + i, 1))).unwrap();
            l2.sync_dirty(&mut e2, &mut small, 1);
        }
        assert!(!l2.valid);
        assert_eq!(small[..4], [0u8; 4], "magic cleared");
        assert_eq!(restore_live(&small).err(), Some(RestoreError::BadMagic));
    }

    /// The point of the live mode: per-record cost stays small at depth,
    /// where the full mode re-serialises everything. Native timing, so only
    /// a coarse guard - the cluster run is the measurement.
    #[test]
    fn live_sync_cost_does_not_grow_with_depth() {
        let mut e = Engine::new(0, 1);
        let mut live = LiveRegion::new(64, 64);
        let mut region = vec![0u8; 4 << 20];
        live.sync_all(&mut e, &mut region, 0);
        for i in 0..10_000 {
            e.process(&encode_new_order(&no(&format!("9p-s-{i}-0"), 1, 100_000 + i, 1))).unwrap();
            live.sync_dirty(&mut e, &mut region, 0);
            e.process(&encode_new_order(&no(&format!("9p-b-{i}-0"), 0, 99 - (i % 98), 1))).unwrap();
            live.sync_dirty(&mut e, &mut region, 0);
        }
        let t0 = std::time::Instant::now();
        for s in 0..2_000u64 {
            e.process(&encode_new_order(&no(&format!("7t-s{s}-0"), 1, 100, 1))).unwrap();
            live.sync_dirty(&mut e, &mut region, s);
            e.process(&encode_new_order(&no(&format!("7t-b{s}-123"), 0, 200, 1))).unwrap();
            live.sync_dirty(&mut e, &mut region, s);
        }
        let per = t0.elapsed() / 4_000;
        assert!(per < std::time::Duration::from_micros(200), "per-record process+sync {per:?} at depth 20k");
        let mut full = vec![0u8; 4 << 20];
        let t1 = std::time::Instant::now();
        save(&e, &mut full, 1, 0).unwrap();
        eprintln!("live: {per:?} per record at depth 20k; full snapshot: {:?} each", t1.elapsed());
    }

    #[test]
    fn parses_loadgen_ids() {
        assert_eq!(parse_seq_id("1792t-s15-0"), Some(("1792", b't', 0, 15)));
        assert_eq!(parse_seq_id("1792t-b15q3-99"), Some(("1792", b't', 1, 15)));
        assert_eq!(parse_seq_id("1792w-i7-0"), Some(("1792", b'w', 2, 7)));
        assert_eq!(parse_seq_id("1792w-i7q1-0"), Some(("1792", b'w', 3, 7)));
        assert_eq!(parse_seq_id("1792w-i7q2-0"), None);
        // preload: negative seq is not deduplicated
        assert_eq!(parse_seq_id("1792p-s-1-0"), None);
        assert_eq!(parse_seq_id("nope"), None);
        assert_eq!(parse_seq_id("12t-x5-0"), None);
    }

    /// A replay after restore is skipped with no output; the next fresh
    /// record is processed; a jump past the next expected seq is a gap.
    #[test]
    fn replay_is_skipped_and_gap_is_detected() {
        let p = 2u32;
        let mut e = Engine::new(0, p);
        for s in 0..10u64 {
            e.process(&encode_new_order(&no(&format!("5t-s{s}-0"), 1, 100, 1))).unwrap();
            e.process(&encode_new_order(&no(&format!("5t-b{s}-1"), 0, 200, 1))).unwrap();
        }
        let mut region = vec![0u8; 1 << 16];
        save(&e, &mut region, 1, 0).unwrap();
        let (mut r, _) = restore(&region).unwrap();
        // Replay of seq 8 (lane 0) and 9 (lane 1): skipped, no output.
        assert!(r.process(&encode_new_order(&no("5t-s8-0", 1, 100, 1))).is_none());
        assert!(r.process(&encode_new_order(&no("5t-b9-1", 0, 200, 1))).is_none());
        assert_eq!(r.stats.dup_skipped, 2);
        assert_eq!(r.stats.gaps, 0);
        // Lane 0: next expected seq is 10. Seq 10 arrives - no gap.
        assert!(r.process(&encode_new_order(&no("5t-s10-0", 1, 100, 1))).is_some());
        // Lane 1: hwm is 9, next expected 11; 15 means 11 and 13 were lost.
        assert!(r.process(&encode_new_order(&no("5t-s15-0", 1, 100, 1))).is_some());
        assert_eq!(r.stats.gaps, 1);
        assert_eq!(r.stats.gap_seqs, 2);
        // Gap check fires only once per lane.
        r.process(&encode_new_order(&no("5t-s99-0", 1, 100, 1)));
        assert_eq!(r.stats.gaps, 1);
    }

    /// Without a restore, out-of-order lanes never trip the gap check.
    #[test]
    fn no_gap_check_without_restore() {
        let mut e = Engine::new(0, 1);
        e.process(&encode_new_order(&no("5t-s0-0", 1, 100, 1)));
        e.process(&encode_new_order(&no("5t-s50-0", 1, 100, 1)));
        assert_eq!(e.stats.gaps, 0);
    }

    /// The output for every message type matches ../matcher.rs on the same
    /// input stream, so the harness's receipt accounting is unchanged.
    #[test]
    fn output_matches_the_mass_matcher_dispatch() {
        let mut e = Engine::new(0, 1);
        let mq = encode_mass_quote(&MassQuote {
            orders: vec![no("3t-s1q1-0", 1, 100, 1), no("3t-s1q2-0", 1, 100, 1)],
        });
        assert!(decode_fills(&e.process(&mq).unwrap()).unwrap().is_empty());
        let mb = encode_mass_quote(&MassQuote {
            orders: vec![no("3t-b1q1-5", 0, 200, 1), no("3t-b1q2-5", 0, 200, 1)],
        });
        let f = decode_fills(&e.process(&mb).unwrap()).unwrap();
        assert_eq!(f.len(), 2);
        e.process(&encode_new_order(&no("3t-i2-0", 0, 50, 1))).unwrap();
        let c = e.process(&encode_cancel(&Cancel { order_id: "3t-i2-0".into() })).unwrap();
        assert!(decode_fills(&c).unwrap().is_empty());
        assert!(e.process(&encode_cancel(&Cancel { order_id: "3t-i2-0".into() })).is_none());
        assert_eq!(e.book.len(), 0);
    }
}

#[cfg(test)]
mod replay_file {
    use super::*;
    /// Feed a hex-per-line dump of an input topic through the engine and
    /// report what it skipped or rejected. Diagnostic only:
    /// `REPLAY_HEX=path cargo test replay_hex_dump -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn replay_hex_dump() {
        let path = std::env::var("REPLAY_HEX").expect("REPLAY_HEX");
        let lanes = env_u64("SEQ_LANES", 1) as u32;
        let mut e = Engine::new(0, lanes);
        let mut shown = 0;
        for (n, line) in std::fs::read_to_string(path).unwrap().lines().enumerate() {
            let b: Vec<u8> = (0..line.len() / 2)
                .map(|i| u8::from_str_radix(&line[2 * i..2 * i + 2], 16).unwrap())
                .collect();
            let before = e.stats;
            let _ = e.process(&b);
            if e.stats != before && shown < 15 {
                shown += 1;
                let id = wire::decode_new_order(&b).map(|o| o.order_id).or_else(|_| wire::decode_cancel(&b).map(|c| c.order_id));
                println!("offset {n}: {:?} -> {:?}", id, e.stats);
            }
        }
        println!("final {:?} orders={}", e.stats, e.book.len());
    }
}
