//! Zero-emit relay consumer: subscribes to the relay's push fan-out,
//! fully receives and parses every delivered record, and writes
//! **nothing**.
//!
//! # Why this exists
//!
//! It is the N-1 filler for `run-e2-throughput.sh MODE=fanout`. Round 2 on
//! 2026-08-29 was run with `relay-probe` in that role, and every one of the
//! N-1 unmeasured consumers therefore *produced* a record per delivery. At
//! fanout 50 that was ~33 million extra writes in a 30s window (~1.1M
//! records/sec); at fanout 250, ~106 million. The cluster saturated on the
//! benchmark's own output rather than on relay dispatch, so the ladder
//! measured the harness. See bench/RESULTS.md, "Round 2 ... FAILED AS
//! DESIGNED".
//!
//! # What it isolates, and what it does not
//!
//! It does **the same decode work as `relay-probe`** and then discards the
//! result, so the only difference between the two guests is the write. That
//! is deliberate: it keeps the filler consumers realistic (they receive,
//! they parse) while removing the one cost that was destroying the
//! experiment, and it makes `relay-probe` vs `relay-sink` a genuine
//! one-variable comparison if the write cost itself is ever the question.
//!
//! It does **not** make the consumer free. Each instance still owns a wasm
//! VM, still has its pending queue filled by `relay::service::push()`, and
//! still has to be scheduled on its shard before it can drain. Those are the
//! costs the fanout ladder is meant to measure, which is why they are kept.
//!
//! No clock read, unlike `relay-probe`: that read exists only to produce a
//! latency sample, and this guest emits none. One fewer host call per record
//! than `relay-probe`, which is worth knowing when comparing their CPU.

use std::cell::Cell;
use std::convert::Infallible;

use redpanda_transform_sdk::{on_record_written, RecordWriter, WriteEvent};

/// Minimal decode of the fill-list wire format, matching `relay-probe`'s.
/// Duplicated rather than shared through a lib target, the same way
/// `relay-probe` and `position-tracker` each duplicate it.
///
/// Returns the number of fills parsed. The count is the point: it is what
/// keeps the parse from being dead-code-eliminated, and it is what lets the
/// guest assert it actually received a well-formed payload rather than
/// silently accepting garbage - the failure mode that made `on_push`'s
/// `create_validated` drop records invisibly during test development.
fn count_fills(buf: &[u8]) -> Result<usize, ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let count = u16::from_be_bytes(buf[0..2].try_into().unwrap());
    let mut rest = &buf[2..];
    let mut seen = 0usize;
    for _ in 0..count {
        let (_aggressor_id, r) = skip_string(rest)?;
        let (_resting_id, r) = skip_string(r)?;
        let (_aggressor_party, r) = skip_string(r)?;
        let (_resting_party, r) = skip_string(r)?;
        if r.len() < 16 {
            return Err(());
        }
        rest = &r[16..];
        seen += 1;
    }
    // The trailing 8-byte matched_at_nanos the matcher stamps
    // unconditionally, present even when the fill list is empty.
    if rest.len() < 8 {
        return Err(());
    }
    Ok(seen)
}

fn skip_string(buf: &[u8]) -> Result<((), &[u8]), ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let n = u16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
    let buf = &buf[2..];
    if buf.len() < n {
        return Err(());
    }
    Ok(((), &buf[n..]))
}

fn main() {
    // Kept across invocations so the parse result is observably used. A
    // release build with lto=true will otherwise happily delete the whole
    // decode, and this guest would then stop representing a consumer that
    // reads its input at all.
    //
    // A Cell, not a plain `mut`: on_record_written takes an `Fn`, so a
    // captured local cannot be assigned. Interior mutability is the fix, and
    // it is free here - wasm32 is single-threaded, so there is no atomic.
    let fills_seen: Cell<u64> = Cell::new(0);
    on_record_written(
        move |event: WriteEvent, _writer: &mut RecordWriter| -> Result<(), Infallible> {
            if let Some(value) = event.record.value() {
                if let Ok(n) = count_fills(value) {
                    fills_seen.set(fills_seen.get().wrapping_add(n as u64));
                }
            }
            // Deliberately no writer.write(). That absence is the entire
            // point of this guest.
            Ok(())
        },
    );
}
