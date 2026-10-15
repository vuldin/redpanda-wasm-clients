# Wire format

A fixed-layout binary encoding. All integers are big-endian, and there's no
framing beyond what's described here: no schema registry, no length prefix on
the message as a whole, and no versioning. The payload is exactly what a Kafka
record value contains.

Strings are length-prefixed as `[len: u16][len bytes of UTF-8]`. This document
calls that `str`.

## Why it isn't JSON or protobuf

This encoding keeps serialization off the measured path. The benchmark these
clients were built for reports latencies in the hundreds of microseconds, so a
codec costing tens of microseconds per record would be a material fraction of
the number being measured. A fixed layout decodes with bounds checks and
`from_be_bytes`, and costs about nothing.

## Why this document matters

The format has two independent implementations: `rust/src/wire.rs` in this
repo, which is the guest side, and a Go decoder in the benchmark harness,
which the load generator uses to correlate receipts with sends. While both
lived in one repository this was an internal invariant a single test could
protect. Split across two repositories it's a contract, and this file is the
contract. Change it here first, then both implementations.

## Messages

### NewOrder, input to the matcher

| field | type | notes |
|---|---|---|
| type | `u8` | `0` |
| order_id | `str` | |
| participant | `str` | |
| side | `u8` | `0` = buy, `1` = sell |
| price | `i64` | |
| qty | `i64` | |

### Cancel, input to the matcher

| field | type | notes |
|---|---|---|
| type | `u8` | `1` |
| order_id | `str` | |

### MassQuote, input to the matcher

One record carrying several orders, the way an exchange's mass-quote message
does. It exists so orders/sec and records/sec can be driven independently: the
per-record cost of the transform output-write path is what sets the rate
ceiling, so N orders in one record amortise that cost over N orders instead of
paying it once per order. It is also what makes our orders/sec comparable to a
customer's, since they count orders and we counted records.

| field | type | notes |
|---|---|---|
| type | `u8` | `2` |
| count | `u16` | number of orders; `0` is legal |
| orders | `count` x order body | the NewOrder layout above, **without** its type byte |

Three rules, each of which has a test on both sides:

- **A count larger than the bytes supplied is an error, not a short envelope.**
  A truncated record must never be processed as if it were complete.
- **Trailing bytes after the last order are filler and must be ignored**, same
  as every other message here.
- **A `count=1` envelope's body is byte-identical to what NewOrder encodes,
  minus the type byte.** This is what keeps single-order and mass figures
  comparable, so both repositories assert it directly.

The separate type byte is deliberate rather than extending NewOrder with "if
bytes remain, parse another order". Padding is legal filler everywhere else in
this format, so that extension would make a padded single-order record from a
published run decode as an envelope full of garbage orders. With a distinct
type byte every existing decoder fails loudly instead.

Note what batching necessarily costs: one record has one send instant, earns
one `matched_at_nanos` and one receipt. Per-order ids inside an envelope buy
per-order *delivery accounting*, never per-order latency. Any "per-order
latency" under mass quotes is an envelope latency quoted N times.

### Fills, output of the matcher

No leading type byte. A fills payload is a count followed by that many fill
records:

| field | type | notes |
|---|---|---|
| count | `u16` | number of fills that follow |
| per fill: | | repeated `count` times |
| aggressor_id | `str` | |
| resting_id | `str` | |
| aggressor_party | `str` | |
| resting_party | `str` | |
| price | `i64` | |
| qty | `i64` | |

A resting order that crosses nothing produces a payload with `count = 0`. It's
still emitted, since the relay pushes every transformed record, so a consumer
has to handle an empty fill list rather than treating it as an error.

## The trailing timestamp, and why decoders have to ignore it

The matcher appends 8 extra bytes after the last fill:

| field | type | notes |
|---|---|---|
| matched_at_nanos | `u64` | wall clock, nanoseconds since the Unix epoch, taken inside the guest immediately after matching |

Those bytes are outside `count`. Both decoders loop exactly `count` times and
stop, so a decoder that doesn't know about the timestamp never reads it and is
unaffected. That's the point: it lets a latency probe downstream of the
matcher separate match-execution time from delivery time without changing the
format for anyone else.

Two things that are easy to get wrong:

- A decoder must not treat trailing bytes as corruption. Validating that the
  buffer is fully consumed after `count` fills will reject every real payload.
- `matched_at_nanos` is a wall clock read inside the guest, on the broker.
  Comparing it against a timestamp taken on a client machine is a cross-clock
  comparison, and it's why the harness's `match` stage reads negative: the
  matcher stamps after the record is appended to the leader's log, while the
  producer's `acks=all` acknowledgement returns several hundred microseconds
  later. That's a real ordering rather than an error, but it means the value
  must never be subtracted from a client-side timestamp and quoted as a
  duration.

## Conformance

The harness repository carries decoder tests against fixtures produced by the
Rust guest here. If you change anything above, regenerate them. A format change
that passes both test suites separately can still break the pair.

Two fixtures, covering both directions:

- `wire/testdata/fills-from-rust-guest.hex` (output direction) - produced by a
  running guest, checked in as bytes.
- `wire/testdata/mass-quote-from-rust-guest.hex` (input direction) - produced
  by this repository's encoder. Regenerate with:

  ```
  cd rust && REGEN_WIRE_FIXTURES=1 cargo test emit_mass_quote_fixture
  cp testdata/mass-quote-from-rust-guest.hex \
     ../../redpanda-wasm-bench/wire/testdata/
  ```

  Without the env var the same test asserts the checked-in bytes still match
  this encoder, so the fixture cannot go stale silently. The harness pins the
  same bytes in *both* directions - it decodes them and asserts its own encoder
  reproduces them - because the harness is the encoder in production and an
  encoder bug is exactly what a decode-only test cannot see.

Never hand-edit either fixture. Editing one makes the test agree with whichever
side is wrong, which is the failure these files exist to prevent.
