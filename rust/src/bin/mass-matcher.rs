//! A matcher guest that accepts mass quotes as well as single orders.
//!
//! Why this is a separate binary rather than a branch added to
//! ../main.rs: `wasm-matcher.wasm`'s md5 is the comparability anchor for
//! every published figure - run fingerprints record it, and the claim that
//! an in-broker and an external arm ran *the same* matcher is exactly that
//! hash being equal. Rebuilding that module with an extra branch in it
//! would mean no future run could be byte-compared to a published one, so
//! the published module is left alone (a copy is checked in under
//! ../../testdata/published-modules/) and the envelope work lives here.
//!
//! This guest handles both type 0 and type 2, so one module can run the
//! single-order control level and the mass levels in the same sweep. That
//! matters: comparing a mass level against the published baseline alone
//! cannot separate "envelopes are faster per order" from "this binary is
//! different", and the N=1 level run with THIS module is what isolates the
//! second effect.
//!
//! Everything else - the wire format, the book, the risk checker, the
//! trailing matched_at_nanos - is shared source with the published guest
//! via the #[path] declarations below, not a reimplementation, so an
//! envelope's orders are treated identically to the same orders sent one
//! per record. `matcher.rs` asserts that equivalence directly.

// Same rationale as ../main.rs: these modules mirror the Go
// implementation's full public surface, so some items are exercised only
// by this crate's own tests.
#![allow(dead_code)]

// Shared with ../main.rs. There is no [lib] target in this crate - the
// published binary is `path = "src/main.rs"` - so modules are pulled in by
// path rather than through a library. Adding a [lib] would change the
// published module's codegen, which is the one thing this file exists to
// avoid.
#[path = "../matcher.rs"]
mod matcher;
#[path = "../orderbook.rs"]
mod orderbook;
#[path = "../risk.rs"]
mod risk;
#[path = "../wire.rs"]
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
            // process_any, not process: the latter deliberately still
            // rejects envelopes so the published guest's behaviour is
            // untouched.
            //
            // One output record per input record either way - an envelope
            // of N orders emits ONE fills payload carrying all of their
            // fills, which is what keeps the harness's 1:1 receipt
            // accounting intact. Per-order rejections inside an envelope
            // are contained by process_mass_quote rather than dropping the
            // whole record; a malformed envelope is still dropped, matching
            // this guest's single-order policy of not retrying bytes that
            // would fail the same deterministic way again.
            if let Ok(mut out) = m.borrow_mut().process_any(value, 0) {
                // Trailing 8 bytes after encode_fills' count-prefixed
                // structure, same as the published guest. Note there is
                // exactly ONE of these per record, so under mass quotes it
                // timestamps the envelope, not each order - per-order
                // latency does not exist here by construction.
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
