# redpanda-wasm-clients

Wasm guests for an order-matching workload, and a host that runs any of them
outside the broker as an ordinary Kafka client.

The pair exists so the two can be compared. The same guest module runs in two
places:

- in-broker, deployed as a Redpanda data transform, where its input arrives
  without a fetch and its output can be pushed to other in-broker consumers
- externally, hosted by `wasm-client` here, consuming and producing over the
  Kafka protocol like any other application

Since it's the same module in both cases, byte-identical and verifiable with
`make checksum`, a latency difference between the two says something about
where code executes rather than about two different implementations. That's
why this repo is separate from the benchmark harness that drives it.

## Prerequisites

| requirement | notes |
|---|---|
| Rust toolchain | run `rustup target add wasm32-wasip1`, since the guests are `wasm32-wasip1` |
| Go 1.25+ | for the host only |
| network access on the first build | `rust/Cargo.toml` pulls `redpanda-transform-sdk` from the `redpanda-data/redpanda` git repo rather than crates.io |

Nothing here needs a running Redpanda to build. Running the host against a
cluster needs broker addresses and topics that already exist.

## Build

```sh
make            # guests + host
make build-wasm # guests only -> bin/*.wasm
make build-host # host only   -> bin/wasm-client
make checksum   # md5 of each guest, record this with any measurement
make test       # cargo test, wire format and matching logic
```

## The guests

| module | what it does |
|---|---|
| `wasm-matcher.wasm` | the order matcher. Consumes NewOrder/Cancel, emits fills. This is what the published measurements are of |
| `relay-probe.wasm` | an in-broker consumer that re-emits `<id>:<receipt_nanos>:<matched_at_nanos>`, which is what lets a harness separate match time from delivery time |
| `passthrough.wasm` | zero-work control. Same plumbing, no matching, so it isolates relay cost from guest cost |
| `relay-sink.wasm` | terminal consumer for fan-out load, where only the delivery matters and the output doesn't |
| `position-tracker.wasm` | consumes fills and maintains per-participant position. An example of a stateful relay-sourced consumer |

All five share `rust/src/wire.rs`, `matcher.rs`, `orderbook.rs` and `risk.rs`.

## The host

`host/main.go` embeds wasmtime and implements the guest-facing ABI the
transform SDK expects, so `read_batch_header`, `read_next_record` and
`write_record`. A guest built for in-broker deployment runs unmodified.

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
| `RISK_LIMIT` | `0` | passed to the guest. The in-broker leg sets this via `--var RISK_LIMIT=0`, so match it or the two legs run different logic |
| `PRODUCER_LINGER_MS` | `0` | see below |

### Settings that aren't franz-go's defaults, and why

`PRODUCER_LINGER_MS=0`. franz-go batches produces for up to 10 ms by default.
Under real order flow that's a reasonable throughput trade, but for a latency
measurement where the host emits one fill at a time it's dead time, and a
batch that can never fill early pays close to the whole linger on every
record. This one setting accounted for the majority of every
double-digit-millisecond figure this workload ever produced. It's configurable
rather than hardcoded because a real deployment might legitimately want the
throughput back.

Metadata age and retry backoff, both hardcoded in `host/main.go` rather than
exposed as env vars. franz-go defaults `metadataMinAge` to 5 s, which
rate-limits how soon a client may re-fetch metadata after a `NOT_LEADER`. A
record can therefore wait about 5 s to learn the new leader while the broker
is handling produce in well under a millisecond, and the default retry
backoff of 250 ms escalating to a 5 s cap compounds it. franz-go retries
transparently and succeeds, so the client records zero produce errors and the
whole cost shows up as latency, which makes it easy to misread as a broker
problem. The host sets `MetadataMinAge(100ms)` and a retry backoff that
doubles from 50 ms to a 500 ms cap, both of which sit inside the window a
leadership transfer actually takes. franz-go's floor for `metadataMinAge` is
10 ms.

All-ISR acks. The host produces with `kgo.RequiredAcks(kgo.AllISRAcks())` and
leaves franz-go's idempotent produce on. Both are explicit rather than
inherited, so a comparison against the in-broker leg offers the same
durability rather than a faster but weaker guarantee.

## Wire format

See [`WIRE-FORMAT.md`](WIRE-FORMAT.md). Read it before writing anything that
decodes fills, in particular the trailing `matched_at_nanos` bytes. They sit
outside the fill count, and a strict decoder will wrongly treat them as
corruption.

## What isn't here

There's no benchmark harness, no load generator and no infrastructure in this
repo. Provisioning, measurement and the published results live in
[redpanda-wasm-bench](https://github.com/vuldin/redpanda-wasm-bench). The
split exists so the artifact under test can't be accidentally changed by the
thing measuring it.

An earlier version of this workload also had a Go matcher, Go external clients
and an external Go relay. Those are gone. The relay moved into the broker, and
keeping a second independently written matcher meant any
in-broker-against-external comparison risked measuring the difference between
two implementations rather than between two execution locations.
