//! The wasm guest entrypoint - a Rust reimplementation of
//! ../../cmd/wasm-matcher/main.go, existing solely to measure guest
//! -language overhead (cold start, per-record marginal cost) against
//! the Go entrypoint on identical workload. See ../README.md's "Why a
//! Rust variant" section. Not the project's primary reference client -
//! wasm-orderbook's dual-mode (in-broker/external) design and its
//! actual latency comparison both stay on the Go entrypoints.

// This binary only exercises the NewOrder/no-cross path bench sends,
// but the modules below mirror the Go implementation's full public
// surface (Cancel, Amend, diagnostic error fields, Exposure queries)
// deliberately, since that's what a faithful reimplementation means -
// hence some items only used by this crate's own tests, not main().
#![allow(dead_code)]

mod matcher;
mod orderbook;
mod risk;
mod wire;

use std::cell::RefCell;
use std::convert::Infallible;
use std::time::{SystemTime, UNIX_EPOCH};

use redpanda_transform_sdk::{on_record_written, Record, RecordWriter, WriteEvent};

fn main() {
    let risk_limit: i64 = std::env::var("RISK_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let m = RefCell::new(matcher::Matcher::new(risk_limit));
    on_record_written(
        move |event: WriteEvent, writer: &mut RecordWriter| -> Result<(), Infallible> {
            let Some(value) = event.record.value() else {
                return Ok(());
            };
            // No offset accessor on WrittenRecord in this SDK version -
            // seq=0 falls back to the matcher's own internal counter,
            // which is fine for this benchmark's synthetic, never-crossing
            // workload (see matcher.rs's own doc comment on seq).
            // A malformed or rejected record is dropped, not retried,
            // matching cmd/wasm-matcher's own policy - retrying the same
            // bytes would fail the same deterministic way every time.
            if let Ok(mut out) = m.borrow_mut().process(value, 0) {
                // Trailing 8 bytes, appended after wire::encode_fills'
                // own count-prefixed structure - decode_fills stops
                // reading after exactly `count` fills, so this is
                // invisible to any consumer that doesn't know to look
                // for it (byte-compatible with wire.go's own decoder).
                // Lets a latency probe downstream of this write
                // (relay-probe.rs) isolate match-execution time from
                // produce and relay-delivery time, instead of only
                // ever seeing one lumped end-to-end number.
                let matched_at_nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                out.extend_from_slice(&matched_at_nanos.to_be_bytes());
                let _ = writer.write(&Record::new(None, Some(out)));
            }
            Ok(())
        },
    );
}
