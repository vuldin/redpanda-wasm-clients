//! Server-side filtering for FIX gateways: a stateless demux.
//!
//! Every gateway reading one "mega events" topic and discarding what is not
//! its own costs N x the topic's bytes in broker egress, and gateway CPU spent
//! throwing records away. This transform routes each event to the one gateway
//! it belongs to, so each gateway reads only its slice: egress drops from
//! N x F to ~F. Nothing is fanned out - output volume equals input volume - so
//! the measure of success is bytes and CPU per gateway, not latency.
//!
//! The gateway is read from the event itself, so there is no state: no lookup
//! table, nothing to snapshot or restore. Event values, big-endian:
//!
//!   [0x47 'G'][gateway:u16][seq:u64][send_nanos:u64][padding...]
//!
//! and are written out unchanged (the consumer measures end to end from
//! send_nanos). Anything else is dropped.
//!
//! Two output shapes, chosen with the MODE environment variable at deploy:
//!
//!   MODE=topics  one output topic per gateway, named "<TOPIC_PREFIX><id>"
//!                (TOPIC_PREFIX defaults to "gw-"). Exact, but a transform has
//!                at most 8 output topics (a broker constant), so 8 gateways.
//!   MODE=keyed   one output topic, partition chosen by key = the gateway id as
//!                decimal text. Any gateway count works; a gateway reads the
//!                partition murmur2(key) % P resolves to, and gets the traffic
//!                of any gateway whose id lands on the same partition too.
//!
//! The SDK's WriteOptions can only name a topic, so keyed writes go through the
//! host's write_record_with_options directly with the partition-key option
//! (0x02), encoding the record the way the SDK does.

use std::cell::RefCell;
use std::convert::Infallible;

use redpanda_transform_sdk::{on_record_written, Record, RecordWriter, WriteEvent, WriteOptions};

const MAGIC: u8 = 0x47;
const HEADER: usize = 1 + 2 + 8 + 8;

/// The gateway an event belongs to, or None for anything that is not one.
pub fn gateway_of(value: &[u8]) -> Option<u16> {
    if value.len() < HEADER || value[0] != MAGIC {
        return None;
    }
    Some(u16::from_be_bytes([value[1], value[2]]))
}

/// Kafka-style zigzag varint, as the transform SDK writes it.
pub fn write_varint(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    while z >= 0x80 {
        out.push((z as u8) | 0x80);
        z >>= 7;
    }
    out.push(z as u8);
}

/// A length-prefixed buffer; None is length -1.
pub fn write_sized(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        Some(b) => {
            write_varint(out, b.len() as i64);
            out.extend_from_slice(b);
        }
        None => write_varint(out, -1),
    }
}

/// The record payload the host expects: key, value, no headers.
pub fn encode_record(out: &mut Vec<u8>, key: Option<&[u8]>, value: Option<&[u8]>) {
    out.clear();
    write_sized(out, key);
    write_sized(out, value);
    write_varint(out, 0);
}

/// Write options with only a partition key (option 0x02).
pub fn encode_partition_key(out: &mut Vec<u8>, key: &[u8]) {
    out.clear();
    out.push(0x02);
    write_sized(out, Some(key));
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "redpanda_transform")]
extern "C" {
    #[link_name = "write_record_with_options"]
    fn host_write_record_with_options(buf: *const u8, len: u32, opts: *const u8, opts_len: u32) -> i32;
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn host_write_record_with_options(_: *const u8, len: u32, _: *const u8, _: u32) -> i32 {
    len as i32
}

struct Keyed {
    record: Vec<u8>,
    options: Vec<u8>,
    keys: Vec<Vec<u8>>,
}

impl Keyed {
    fn write(&mut self, gateway: u16, value: &[u8]) {
        let g = gateway as usize;
        while self.keys.len() <= g {
            self.keys.push(self.keys.len().to_string().into_bytes());
        }
        let key = &self.keys[g];
        encode_record(&mut self.record, Some(key), Some(value));
        encode_partition_key(&mut self.options, key);
        // A failed write is the host's to report: it fails the transform.
        let _ = unsafe {
            host_write_record_with_options(
                self.record.as_ptr(),
                self.record.len() as u32,
                self.options.as_ptr(),
                self.options.len() as u32,
            )
        };
    }
}

fn main() {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "topics".into());
    let prefix = std::env::var("TOPIC_PREFIX").unwrap_or_else(|_| "gw-".into());
    match mode.as_str() {
        "keyed" => {
            // The SDK's callback is a Fn; the reused buffers live in a RefCell.
            let keyed = RefCell::new(Keyed { record: Vec::new(), options: Vec::new(), keys: Vec::new() });
            on_record_written(move |event: WriteEvent, _w: &mut RecordWriter| -> Result<(), Infallible> {
                if let Some(value) = event.record.value() {
                    if let Some(g) = gateway_of(value) {
                        keyed.borrow_mut().write(g, value);
                    }
                }
                Ok(())
            });
        }
        _ => {
            let topics: RefCell<Vec<String>> = RefCell::new(Vec::new());
            on_record_written(move |event: WriteEvent, w: &mut RecordWriter| -> Result<(), Infallible> {
                let Some(value) = event.record.value() else { return Ok(()) };
                let Some(g) = gateway_of(value) else { return Ok(()) };
                let g = g as usize;
                let mut topics = topics.borrow_mut();
                while topics.len() <= g {
                    let next = format!("{prefix}{}", topics.len());
                    topics.push(next);
                }
                let _ = w.write_with_options(
                    &Record::new(None, Some(value.to_vec())),
                    WriteOptions::to_topic(&topics[g]),
                );
                Ok(())
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(gateway: u16) -> Vec<u8> {
        let mut v = vec![MAGIC];
        v.extend_from_slice(&gateway.to_be_bytes());
        v.extend_from_slice(&7u64.to_be_bytes());
        v.extend_from_slice(&123u64.to_be_bytes());
        v.extend_from_slice(&[0u8; 32]);
        v
    }

    #[test]
    fn reads_the_gateway_and_rejects_everything_else() {
        assert_eq!(gateway_of(&event(19)), Some(19));
        assert_eq!(gateway_of(&event(0)), Some(0));
        assert_eq!(gateway_of(&event(19)[..HEADER - 1]), None);
        let mut wrong = event(3);
        wrong[0] = 0;
        assert_eq!(gateway_of(&wrong), None);
    }

    /// The values Kafka's own zigzag varint produces.
    #[test]
    fn varint_matches_kafka() {
        for (v, want) in [(0i64, vec![0u8]), (-1, vec![1]), (1, vec![2]), (63, vec![126]),
                          (64, vec![0x80, 0x01]), (300, vec![0xd8, 0x04])] {
            let mut out = Vec::new();
            write_varint(&mut out, v);
            assert_eq!(out, want, "varint({v})");
        }
    }

    /// Byte layout the host parses: sized key, sized value, zero headers; and
    /// options [0x02][sized key].
    #[test]
    fn record_and_options_layout() {
        let mut rec = Vec::new();
        encode_record(&mut rec, Some(b"12"), Some(b"abc"));
        assert_eq!(rec, vec![4, b'1', b'2', 6, b'a', b'b', b'c', 0]);
        let mut none = Vec::new();
        encode_record(&mut none, None, Some(b""));
        assert_eq!(none, vec![1, 0, 0]);
        let mut opts = Vec::new();
        encode_partition_key(&mut opts, b"12");
        assert_eq!(opts, vec![0x02, 4, b'1', b'2']);
    }
}
