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
//!    region with the broker and keeps a crc-checked snapshot of its book,
//!    risk exposure and replay high-water marks in it. The broker copies the
//!    region into the input partition's raft group every 5s and writes it
//!    back before the first record after a restart; the guest rebuilds from
//!    it on that first record. `STATE=off` (the default) registers nothing,
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
}

impl Exposure {
    fn new(limit: i64) -> Self {
        Self {
            limit,
            map: FnvMap::default(),
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
        }
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
// guest entrypoint
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "redpanda_wasm_shared_memory")]
extern "C" {
    fn register_region(ptr: u32, len: u32) -> i32;
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
        let Some(region) = self.region.as_deref() else {
            log("STATE off");
            return;
        };
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

    fn after_record(&mut self) {
        let now = now_nanos();
        if let Some(region) = self.region.as_deref_mut() {
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
                "STATE stats gen={} orders={} bytes={} saves={} save_us_avg={} save_us_max={} dup_skipped={} gaps={} gap_seqs={} rejected={}",
                self.generation,
                self.engine.book.len(),
                self.last_bytes,
                self.saves,
                if self.saves > 0 { self.save_ns_sum / self.saves / 1000 } else { 0 },
                self.save_ns_max / 1000,
                s.dup_skipped,
                s.gaps,
                s.gap_seqs,
                s.rejected
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
        let rc = unsafe { register_region(r.as_ptr() as u32, r.len() as u32) };
        if rc != 0 {
            panic!("register_region failed: {rc}");
        }
        Some(r)
    } else {
        None
    };
    let g = RefCell::new(Guest {
        engine: Engine::new(risk_limit, lanes),
        region,
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
