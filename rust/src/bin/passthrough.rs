//! The zero-work control for E2.3 in bench/SCALING-TEST-PLAN.md.
//!
//! E2.1 measures CPU-microseconds per record for the matcher, but that number
//! lumps two costs that scale for different reasons: the engine's own
//! per-record cost (host/guest handoff, wire decode, the output write, the
//! relay push) and the guest's actual work (order book insert, cross detection,
//! risk check). A capacity model needs them separately - engine overhead is a
//! floor no workload can get under, while guest work is whatever the customer's
//! own logic costs and varies per deployment.
//!
//! So this guest does the least possible while still producing output the rest
//! of the harness can measure: decode the order id, emit a single synthetic
//! fill carrying that id, stamp matched_at. No order book, no risk check, no
//! state of any kind. Run it in the matcher's place with everything else
//! identical, and the difference between the two runs' cpu_micros_per_record is
//! the matching logic's own cost.
//!
//! Wire-compatible with rust/src/bin/relay-probe.rs's decoder on purpose: it
//! reads count-prefixed fills and then the trailing 8-byte matched_at, exactly
//! as it does for the real matcher's output, so no probe-side change is needed.
//!
//! The encode/decode below is deliberately duplicated rather than shared with
//! ../wire.rs via a lib target - same reasoning as position-tracker.rs and
//! relay-probe.rs, and it keeps this binary's own per-record cost free of any
//! abstraction the matcher does not also pay.

use std::convert::Infallible;
use std::time::{SystemTime, UNIX_EPOCH};

use redpanda_transform_sdk::{on_record_written, Record, RecordWriter, WriteEvent};

const MSG_NEW_ORDER: u8 = 0;

/// Reads just the order id out of ../wire.rs's NewOrder layout:
/// `[0x00][len:2][orderID][len:2][participant][side:1][price:8][qty:8]`.
/// Trailing bytes past the fixed layout are ignored, which is what lets
/// cmd/loadgen pad records to a target size without changing semantics.
fn decode_order_id(buf: &[u8]) -> Option<&[u8]> {
    if buf.first() != Some(&MSG_NEW_ORDER) || buf.len() < 3 {
        return None;
    }
    let n = u16::from_be_bytes(buf[1..3].try_into().ok()?) as usize;
    let rest = &buf[3..];
    if rest.len() < n {
        return None;
    }
    Some(&rest[..n])
}

/// One fill, per ../wire.rs's layout:
/// `[count:2]` then `[len][aggressorID][len][restingID][len][aggressorParty][len][restingParty][price:8][qty:8]`.
/// Only aggressorID is populated - relay-probe correlates on it and ignores the
/// rest, and leaving the other three empty keeps this guest's output size (and
/// so its write cost) at the minimum the format allows.
fn encode_one_fill(aggressor_id: &[u8], matched_at_nanos: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 8 + aggressor_id.len() + 16 + 8);
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&(aggressor_id.len() as u16).to_be_bytes());
    out.extend_from_slice(aggressor_id);
    for _ in 0..3 {
        out.extend_from_slice(&0u16.to_be_bytes()); // restingID, both parties
    }
    out.extend_from_slice(&0i64.to_be_bytes()); // price
    out.extend_from_slice(&0i64.to_be_bytes()); // qty
    out.extend_from_slice(&matched_at_nanos.to_be_bytes());
    out
}

fn main() {
    on_record_written(
        move |event: WriteEvent, writer: &mut RecordWriter| -> Result<(), Infallible> {
            let Some(value) = event.record.value() else {
                return Ok(());
            };
            // The resting (sell) leg of each crossing pair decodes fine here
            // too - unlike the real matcher, this guest has no book, so it
            // cannot tell an aggressor from a resting order and emits a fill
            // for both. That doubles the receipt count relative to a matcher
            // run, which is why E2.3 compares cpu_micros_per_record (a per
            // record figure) rather than total CPU.
            let Some(order_id) = decode_order_id(value) else {
                return Ok(());
            };
            let matched_at_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let _ = writer.write(&Record::new(
                None,
                Some(encode_one_fill(order_id, matched_at_nanos)),
            ));
            Ok(())
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirrors rust/src/bin/relay-probe.rs's decode_aggressor_ids exactly. If
    /// this drifts from that function, the passthrough's output stops
    /// correlating and E2.3 silently measures nothing - so the contract is
    /// asserted here rather than discovered on a cluster.
    fn probe_decode(buf: &[u8]) -> Result<(Vec<String>, u64), ()> {
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
        Ok((ids, u64::from_be_bytes(rest[0..8].try_into().unwrap())))
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

    fn encode_new_order(order_id: &str, participant: &str) -> Vec<u8> {
        let mut b = vec![MSG_NEW_ORDER];
        b.extend_from_slice(&(order_id.len() as u16).to_be_bytes());
        b.extend_from_slice(order_id.as_bytes());
        b.extend_from_slice(&(participant.len() as u16).to_be_bytes());
        b.extend_from_slice(participant.as_bytes());
        b.push(0); // side
        b.extend_from_slice(&100i64.to_be_bytes()); // price
        b.extend_from_slice(&1i64.to_be_bytes()); // qty
        b
    }

    #[test]
    fn output_decodes_with_the_probes_decoder() {
        let id = "1787938000000000000t-b42-1787938000123456789";
        let input = encode_new_order(id, "buyer");
        let got = decode_order_id(&input).expect("order id should decode");
        assert_eq!(got, id.as_bytes());

        let stamp = 1_787_938_000_999_999_999u64;
        let out = encode_one_fill(got, stamp);
        let (ids, matched_at) = probe_decode(&out).expect("probe must decode our output");
        assert_eq!(ids, vec![id.to_string()]);
        assert_eq!(matched_at, stamp);
    }

    /// cmd/loadgen pads records to a target size for E2.2. Both wire decoders
    /// stop at the fixed layout, so padding must be invisible here too.
    #[test]
    fn trailing_padding_is_ignored() {
        let id = "abc-b1-2";
        let mut input = encode_new_order(id, "buyer");
        input.extend_from_slice(&[b'x'; 512]);
        assert_eq!(decode_order_id(&input).unwrap(), id.as_bytes());
    }

    #[test]
    fn rejects_non_new_order_and_short_buffers() {
        assert!(decode_order_id(&[]).is_none());
        assert!(decode_order_id(&[1, 0, 3]).is_none()); // Cancel, not NewOrder
        assert!(decode_order_id(&[MSG_NEW_ORDER, 0]).is_none()); // truncated length
        assert!(decode_order_id(&[MSG_NEW_ORDER, 0, 9, b'a']).is_none()); // length exceeds buffer
    }
}
