# Wire format

A fixed-layout binary encoding, all integers **big-endian**, no framing beyond
what is described here. There is no schema registry, no length prefix on the
message as a whole, and no versioning — the payload is exactly what a Kafka
record value contains.

Strings are length-prefixed: `[len: u16][len bytes of UTF-8]`. This document
calls that `str`.

## Why it is not JSON or protobuf

This encoding exists to keep serialization off the measured path. The benchmark
these clients were built for reports p50/p90/p99 latencies in the tens to
hundreds of microseconds, so a codec that costs tens of microseconds per record
would be a material fraction of the number being measured. A fixed layout is
decoded with bounds checks and `from_be_bytes` and costs approximately nothing.

## Why this document matters

The format has **two independent implementations**: `rust/src/wire.rs` in this
repo (the guest side) and a Go decoder in the benchmark harness (the load
generator, which decodes fills to correlate receipts with sends). While both
lived in one repository this was an internal invariant that a single test could
protect. Split across two repositories it is a contract, and this file is the
contract. **Change it here first, then both implementations.**

## Messages

### NewOrder — input to the matcher

| field | type | notes |
|---|---|---|
| type | `u8` | `0` |
| order_id | `str` | |
| participant | `str` | |
| side | `u8` | `0` = buy, `1` = sell |
| price | `i64` | |
| qty | `i64` | |

### Cancel — input to the matcher

| field | type | notes |
|---|---|---|
| type | `u8` | `1` |
| order_id | `str` | |

### Fills — output of the matcher

No leading type byte. A fills payload is a count followed by that many fill
records:

| field | type | notes |
|---|---|---|
| count | `u16` | number of fills that follow |
| *per fill:* | | repeated `count` times |
| aggressor_id | `str` | |
| resting_id | `str` | |
| aggressor_party | `str` | |
| resting_party | `str` | |
| price | `i64` | |
| qty | `i64` | |

A resting order that crosses nothing produces a payload with `count = 0`. It is
still emitted — the relay pushes every transformed record — so a consumer must
handle an empty fill list rather than treating it as an error.

## The trailing timestamp, and why decoders must ignore it

The matcher appends **8 extra bytes after the last fill**:

| field | type | notes |
|---|---|---|
| matched_at_nanos | `u64` | wall clock, nanoseconds since the Unix epoch, taken inside the guest immediately after matching |

**These bytes are outside `count`.** Both decoders loop exactly `count` times
and stop, so a decoder that does not know about the timestamp never reads it and
is unaffected. That is deliberate: it lets a latency probe downstream of the
matcher separate match-execution time from delivery time without changing the
format for anyone else.

Two consequences worth stating plainly, because they are easy to get wrong:

- **A decoder must not treat trailing bytes as corruption.** Validating that the
  buffer is fully consumed after `count` fills will reject every real payload.
- **`matched_at_nanos` is a wall clock read inside the guest, on the broker.**
  Comparing it against a timestamp taken on a client machine is a cross-clock
  comparison. In the published measurements this is exactly why the `match`
  stage reads *negative*: the matcher stamps after the record is appended to the
  leader's log, while the producer's `acks=all` acknowledgement returns
  several hundred microseconds later. That is a real ordering, not an error, but
  it means the value must never be subtracted from a client-side timestamp and
  quoted as a duration.

## Conformance

The harness repository carries a decoder test against a fixture produced by the
Rust guest in this repository. If you change anything above, regenerate that
fixture — a format change that passes both test suites separately can still
break the pair.
