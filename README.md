# redpanda-wasm-clients

Wasm guests for an order-matching workload, and a host that runs any of them
**outside** the broker as an ordinary Kafka client.

The point of the pair is comparison. The same guest module can run in two
places:

- **in-broker**, deployed as a Redpanda data transform, where its input arrives
  without a fetch and its output can be pushed to other in-broker consumers
- **externally**, hosted by `wasm-client` here, consuming and producing over the
  Kafka protocol like any other application

Because it is the *same module* in both cases — byte-identical, verifiable with
`make checksum` — a latency difference between the two is a statement about
where code executes, not about two different implementations. That property is
the whole reason this repo is separate from the benchmark harness that drives
it.

## Prerequisites

| requirement | notes |
|---|---|
| Rust toolchain | `rustup target add wasm32-wasip1` — the guests are `wasm32-wasip1` |
| Go 1.25+ | for the host only |
| network access on first build | `rust/Cargo.toml` pulls `redpanda-transform-sdk` from the `redpanda-data/redpanda` git repo; it is not on crates.io |

Nothing here needs a running Redpanda to build. Running the host against a
cluster needs broker addresses and topics that already exist.

## Build

```sh
make            # guests + host
make build-wasm # guests only -> bin/*.wasm
make build-host # host only   -> bin/wasm-client
make checksum   # md5 of each guest; record this with any measurement
make test       # cargo test (22 tests, wire format and matching logic)
```

## The guests

| module | what it does |
|---|---|
| `wasm-matcher.wasm` | the order matcher: consumes NewOrder/Cancel, emits fills. The subject of the published measurements. |
| `relay-probe.wasm` | an in-broker consumer that re-emits `<id>:<receipt_nanos>:<matched_at_nanos>`, which is what lets a harness separate match time from delivery time |
| `passthrough.wasm` | zero-work control: same plumbing, no matching. Isolates relay cost from guest cost. |
| `relay-sink.wasm` | terminal consumer for fan-out load, where only the delivery matters and the output does not |
| `position-tracker.wasm` | consumes fills and maintains per-participant position; an example of a stateful relay-sourced consumer |

All five share `rust/src/wire.rs`, `matcher.rs`, `orderbook.rs` and `risk.rs`.

## The host

`host/main.go` embeds wasmtime and implements the guest-facing ABI the
transform SDK expects (`read_batch_header`, `read_next_record`, `write_record`),
so a guest built for in-broker deployment runs unmodified.

```sh
KAFKA_BROKERS=10.0.0.1:9092,10.0.0.2:9092 \
INPUT_TOPIC=orders OUTPUT_TOPIC=fills \
CONSUMER_GROUP=wasm-client-1 \
WASM_FILE=bin/wasm-matcher.wasm \
RISK_LIMIT=0 \
PRODUCER_LINGER_MS=0 \
  ./bin/wasm-client
```

| env var | default | notes |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated |
| `INPUT_TOPIC` | `orders` | |
| `OUTPUT_TOPIC` | `fills` | |
| `CONSUMER_GROUP` | `wasm-client` | give each instance its own if you run several |
| `WASM_FILE` | `bin/wasm-matcher.wasm` | any guest above |
| `RISK_LIMIT` | `0` | passed to the guest; the in-broker leg sets this via `--var RISK_LIMIT=0`, so **match it or the two legs run different logic** |
| `PRODUCER_LINGER_MS` | `0` | see below |

### Two settings that are not defaults for tuning reasons

**`PRODUCER_LINGER_MS=0`.** franz-go batches produces for up to 10 ms by
default. Under real order flow that is a reasonable throughput trade; for a
latency measurement where the host emits one fill at a time it is pure dead
time, and a batch that can never fill early pays close to the whole linger on
every record. This one setting accounted for the majority of every
double-digit-millisecond figure this workload ever produced. It is configurable
rather than hardcoded because a real deployment might legitimately want the
throughput back.

**All-ISR acks.** The host produces with `kgo.RequiredAcks(kgo.AllISRAcks())`
and leaves franz-go's idempotent produce on. Both are explicit rather than
inherited so that a comparison against the in-broker leg is offering the same
durability, not a faster but weaker guarantee.

## Wire format

See [`WIRE-FORMAT.md`](WIRE-FORMAT.md). Read it before writing anything that
decodes fills — in particular the trailing `matched_at_nanos` bytes, which sit
outside the fill count and which a strict decoder will wrongly treat as
corruption.

## What is not here

This repo deliberately contains no benchmark harness, no load generator, and no
infrastructure. Provisioning, measurement and the published results live in the
harness repository. The split exists so the artifact under test cannot be
accidentally changed by the thing measuring it.

An earlier version of this workload also had a Go matcher, Go external clients
and an external Go relay. Those are gone: the relay moved into the broker, and
keeping a second, independently written matcher meant any in-broker-vs-external
comparison risked measuring the difference between two implementations rather
than between two execution locations.
