//! In-broker relay-sourced latency probe: deployed N times (under N
//! distinct transform names, same input/output topics, each with
//! `RELAY_SOURCE=1`) to measure real receipt latency for real in-broker
//! wasm consumers of the relay's push fan-out - closing the gap the
//! stub relay (removed) never could: it only ever measured
//! transform-to-relay, with the payload discarded before ever reaching
//! a real subscriber.
//!
//! For each fill delivered, reads a real host clock (WASI, backed by
//! REQ-12a's non-spoofable clock capability - the same mechanism
//! main_push.rs used before it was removed) and writes
//! `"{aggressor_id}:{receipt_nanos}:{matched_at_nanos}"` to its own
//! output - matched_at_nanos is read back from the trailing 8 bytes
//! wasm-matcher-rs's main.rs appends after its own wire::encode_fills
//! output. An external bench tool that already knows each order's own
//! send time (by aggressor_id, since it generated the order) computes
//! `receipt_nanos - send_nanos` for the full round trip, and can now
//! also split it into produce (matcher pickup - send), match
//! (matched_at - matcher pickup) and relay-delivery (receipt -
//! matched_at) instead of only ever seeing one lumped number - one
//! latency sample per probe instance per fill, so N probes reading the
//! same fill stream produce the N-wide distribution a real fan-out
//! workload cares about, not just one.
//!
//! Deliberately minimal: no state, no risk check, no position tracking
//! - isolates relay-delivery latency from any guest-side compute cost,
//! which position-tracker's own hashmap work would otherwise mix in.

use std::convert::Infallible;
use std::time::{SystemTime, UNIX_EPOCH};

use redpanda_transform_sdk::{on_record_written, Record, RecordWriter, WriteEvent};

// Minimal decode of ../wire.rs's fill-list format, keeping only
// aggressor_id (the correlator) - duplicated rather than shared via a
// lib target, same reasoning as position-tracker.rs. Also reads the
// trailing 8-byte matched_at_nanos main.rs appends after the
// count-prefixed fills structure - present regardless of whether the
// fills list itself is empty, since main.rs stamps it unconditionally
// on every processed record.
fn decode_aggressor_ids(buf: &[u8]) -> Result<(Vec<String>, u64), ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let count = u16::from_be_bytes(buf[0..2].try_into().unwrap());
    let mut rest = &buf[2..];
    let mut ids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (aggressor_id, r) = read_string(rest)?;
        let (_resting_id, r) = read_string(r)?;
        let (_aggressor_party, r) = read_string(r)?;
        let (_resting_party, r) = read_string(r)?;
        if r.len() < 16 {
            return Err(());
        }
        rest = &r[16..];
        ids.push(aggressor_id);
    }
    if rest.len() < 8 {
        return Err(());
    }
    let matched_at_nanos = u64::from_be_bytes(rest[0..8].try_into().unwrap());
    Ok((ids, matched_at_nanos))
}

fn read_string(buf: &[u8]) -> Result<(String, &[u8]), ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let n = u16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
    let buf = &buf[2..];
    if buf.len() < n {
        return Err(());
    }
    Ok((String::from_utf8_lossy(&buf[..n]).into_owned(), &buf[n..]))
}

fn main() {
    on_record_written(
        move |event: WriteEvent, writer: &mut RecordWriter| -> Result<(), Infallible> {
            let Some(value) = event.record.value() else {
                return Ok(());
            };
            let Ok((ids, matched_at_nanos)) = decode_aggressor_ids(value) else {
                return Ok(());
            };
            if ids.is_empty() {
                // A resting order's own empty fill list - still pushed by
                // the relay (every transformed record is), nothing to
                // measure here.
                return Ok(());
            }
            let receipt_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            for id in ids {
                let out = format!("{}:{}:{}", id, receipt_nanos, matched_at_nanos).into_bytes();
                let _ = writer.write(&Record::new(None, Some(out)));
            }
            Ok(())
        },
    );
}
