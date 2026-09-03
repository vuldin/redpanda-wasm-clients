//! Experimental "downstream consumer as a wasm transform" guest, built
//! to test whether internal stream-processing consumers (position
//! tracking, risk calc, audit logging) are cheaper to run as in-broker
//! transforms than as external Kafka clients - see the fanout-relay
//! design doc's "Option C" discussion. Not a project deliverable, not a
//! faithful reimplementation of anything - self-contained on purpose so
//! N copies can be deployed under different names without touching the
//! matcher's own module structure.
//!
//! Reads the same fills wire format ../wire.rs defines, maintains a
//! real (if simplified) running net-position-per-participant map in
//! guest memory, and writes a position-update record per fill - real
//! hashmap lookups, string keys, and a write per invocation, so its
//! per-record cost is representative of genuine downstream processing,
//! not a toy identity function.

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;

use redpanda_transform_sdk::{on_record_written, Record, RecordWriter, WriteEvent};

struct Tracker {
    // participant -> net position. Sign convention: aggressor gains
    // +qty, resting party -qty - not real buy/sell semantics (the fill
    // wire format doesn't carry aggressor side), a deliberate
    // simplification since this exists to measure guest compute cost,
    // not to be a correct risk engine.
    positions: HashMap<String, i64>,
}

impl Tracker {
    fn new() -> Self {
        Tracker {
            positions: HashMap::new(),
        }
    }

    // Returns one output record per fill: `participant:new_position`.
    fn process(&mut self, fills_buf: &[u8]) -> Vec<Vec<u8>> {
        let Ok(fills) = decode_fills(fills_buf) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(fills.len() * 2);
        for f in fills {
            let agg = self.positions.entry(f.aggressor_party.clone()).or_insert(0);
            *agg += f.qty;
            out.push(format!("{}:{}", f.aggressor_party, agg).into_bytes());

            let rest = self.positions.entry(f.resting_party.clone()).or_insert(0);
            *rest -= f.qty;
            out.push(format!("{}:{}", f.resting_party, rest).into_bytes());
        }
        out
    }
}

struct Fill {
    aggressor_party: String,
    resting_party: String,
    qty: i64,
}

// Minimal decode of ../wire.rs's fill-list format - duplicated rather
// than shared via a lib target, deliberately: this binary is
// throwaway experiment code, not meant to become permanent project
// infrastructure sharing the matcher's own module tree.
fn decode_fills(buf: &[u8]) -> Result<Vec<Fill>, ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let count = u16::from_be_bytes(buf[0..2].try_into().unwrap());
    let mut rest = &buf[2..];
    let mut fills = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (_aggressor_id, r) = read_string(rest)?;
        let (_resting_id, r) = read_string(r)?;
        let (aggressor_party, r) = read_string(r)?;
        let (resting_party, r) = read_string(r)?;
        if r.len() < 16 {
            return Err(());
        }
        let qty = i64::from_be_bytes(r[8..16].try_into().unwrap());
        rest = &r[16..];
        fills.push(Fill {
            aggressor_party,
            resting_party,
            qty,
        });
    }
    Ok(fills)
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
    let t = RefCell::new(Tracker::new());
    on_record_written(
        move |event: WriteEvent, writer: &mut RecordWriter| -> Result<(), Infallible> {
            let Some(value) = event.record.value() else {
                return Ok(());
            };
            for out in t.borrow_mut().process(value) {
                let _ = writer.write(&Record::new(None, Some(out)));
            }
            Ok(())
        },
    );
}
