// Command wasm-client is a standalone external entrypoint that runs the
// exact same compiled .wasm binary deployed in-broker as a transform -
// not a reimplementation, the identical bytes - hosted by an ordinary
// Go process using wasmtime-go instead of Redpanda's own wasm engine.
//
// This replaces cmd/kafka-matcher as this project's external-client
// entrypoint. The reason to prefer this over a native reimplementation:
// with a native client, any latency difference between "in-broker" and
// "external" is partly attributable to implementation differences (a
// different language, different serialization path) on top of the
// delivery-mechanism difference this project actually exists to
// measure. Running the identical wasm bytes both places isolates that
// one variable precisely - the only difference between the two runs is
// how the guest is hosted, nothing about what it does.
//
// Implements exactly the 5 host functions any redpanda-transform-sdk
// guest imports (module "redpanda_transform"): check_abi_version_2,
// read_batch_header, read_next_record, write_record,
// write_record_with_options. Semantics (sentinel values, buffer
// handling, field meanings) are taken directly from Redpanda's own
// host-side implementation in src/v/wasm/transform_module.cc, not
// guessed - see the comments on each function below for the specific
// citation.
//
// One real, deliberate simplification: Redpanda's real host maps one
// Kafka record batch to one read_batch_header call. This host treats
// each franz-go PollFetches() call's records as one batch instead -
// close enough to real batching to be representative, but not an
// exact reproduction of upstream batch boundaries. Documented here
// rather than silently assumed.
package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v25"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Sentinel return values - exact values from
// src/v/wasm/transform_module.cc's anonymous-namespace constants, not
// reinvented.
const (
	noActiveTransform int32 = -1
	invalidBuffer     int32 = -2
	invalidWrite      int32 = -3
)

// This host doesn't resource-govern multiple co-resident guests the
// way Redpanda's real one does (per_function_memory_limit etc.), so
// there's no real cap to compute for max_input_record_size the way
// transform_module.cc's for_each_record_async does (the largest
// payload actually seen in the batch). A generous fixed value is fine
// here - it's only ever used by the guest to size its own read buffer.
const maxInputRecordSize = 1 << 20 // 1 MiB

type pendingRecord struct {
	offset    int64
	timestamp int64
	attrs     uint8
	payload   []byte
}

// One host.write() call's worth of parsed write_record_with_options
// output - topic/partition-key override, if any, plus the payload.
type writtenRecord struct {
	topic        string // "" means the configured default output topic
	partitionKey []byte // nil means no explicit key
	payload      []byte
}

// The record envelope read_next_record/write_record actually exchange
// is NOT the raw Kafka record value - it's the SDK's own wire format
// (core-sys/src/serde.rs's write_record_payload/read_record_from_payload):
//   sized(key) ++ sized(value) ++ varint(header_count) ++ per header: sized(key) ++ sized(value)
// where "sized" is a zigzag varint length (-1 for None) followed by
// that many bytes if non-negative (varint/src/lib.rs's
// write_sized_buffer/read_sized_buffer). Confirmed against the actual
// SDK source, not guessed - the first version of this file skipped
// this envelope entirely and passed the raw value straight through,
// which the guest's own deserializer correctly rejected as corrupt.

func zigzagEncode(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }
func zigzagDecode(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }

func appendVarint(buf []byte, v int64) []byte {
	u := zigzagEncode(v)
	for u >= 0x80 {
		buf = append(buf, byte(u)|0x80)
		u >>= 7
	}
	return append(buf, byte(u))
}

// decodeSignedVarint mirrors decodeVarint but zigzag-decodes the
// result, matching varint::read (not the unsigned reader used for the
// header-count-adjacent tags in write_record_with_options's own
// options encoding, which is a plain, non-zigzag varint per
// transform_module.cc's ffi::reader - the two encodings coexist in
// this ABI and must not be conflated).
func decodeSignedVarint(buf []byte) (int64, int) {
	u, n := decodeVarint(buf)
	if n == 0 {
		return 0, 0
	}
	return zigzagDecode(u), n
}

func appendSizedBuffer(buf []byte, v []byte, isNone bool) []byte {
	if isNone {
		return appendVarint(buf, -1)
	}
	buf = appendVarint(buf, int64(len(v)))
	return append(buf, v...)
}

// readSizedBuffer returns (value, isNone, bytesConsumed). value is nil
// when isNone.
func readSizedBuffer(buf []byte) ([]byte, bool, int) {
	n, consumed := decodeSignedVarint(buf)
	if consumed == 0 {
		return nil, false, 0
	}
	if n < 0 {
		return nil, true, consumed
	}
	if consumed+int(n) > len(buf) {
		return nil, false, 0
	}
	return buf[consumed : consumed+int(n)], false, consumed + int(n)
}

// encodeRecordEnvelope builds the sized(key)++sized(value)++varint(0
// headers) envelope for one record. This project's guests never set
// headers, so header_count is always encoded as 0 - a real SDK-hosted
// record with headers would need this extended, not silently dropped.
func encodeRecordEnvelope(key, value []byte, keyIsNone, valueIsNone bool) []byte {
	buf := make([]byte, 0, len(key)+len(value)+8)
	buf = appendSizedBuffer(buf, key, keyIsNone)
	buf = appendSizedBuffer(buf, value, valueIsNone)
	buf = appendVarint(buf, 0) // header count
	return buf
}

// decodeRecordEnvelope parses the same format back into key/value -
// the write side's counterpart to encodeRecordEnvelope. Headers, if
// any, are read past but discarded (see the same caveat above).
func decodeRecordEnvelope(buf []byte) (key, value []byte, ok bool) {
	k, kNone, n1 := readSizedBuffer(buf)
	if n1 == 0 {
		return nil, nil, false
	}
	buf = buf[n1:]
	v, vNone, n2 := readSizedBuffer(buf)
	if n2 == 0 {
		return nil, nil, false
	}
	buf = buf[n2:]
	headerCount, n3 := decodeSignedVarint(buf)
	if n3 == 0 {
		return nil, nil, false
	}
	buf = buf[n3:]
	for i := int64(0); i < headerCount; i++ {
		_, _, hn1 := readSizedBuffer(buf)
		if hn1 == 0 {
			return nil, nil, false
		}
		buf = buf[hn1:]
		_, _, hn2 := readSizedBuffer(buf)
		if hn2 == 0 {
			return nil, nil, false
		}
		buf = buf[hn2:]
	}
	if kNone {
		k = nil
	}
	if vNone {
		v = nil
	}
	return k, v, true
}

func main() {
	brokers := envOr("KAFKA_BROKERS", "localhost:9092")
	inputTopic := envOr("INPUT_TOPIC", "orders")
	outputTopic := envOr("OUTPUT_TOPIC", "fills")
	groupID := envOr("CONSUMER_GROUP", "wasm-client")
	wasmFile := envOr("WASM_FILE", "bin/wasm-matcher.wasm")
	lingerMs, _ := strconv.ParseInt(envOr("PRODUCER_LINGER_MS", "0"), 10, 64)

	engine := wasmtime.NewEngine()
	module, err := wasmtime.NewModuleFromFile(engine, wasmFile)
	if err != nil {
		log.Fatalf("wasm-client: loading %s: %v", wasmFile, err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(inputTopic),
		// Same fairness rationale as cmd/kafka-matcher: match the
		// in-broker entrypoint's idempotent-produce durability,
		// don't compare a faster-but-weaker guarantee.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(time.Duration(lingerMs)*time.Millisecond),

		// Recover fast when a partition changes leader.
		//
		// franz-go's defaults are tuned for throughput, not for the tail during
		// a leadership move, and they cost seconds. Measured on a 3-broker
		// cluster during a maintenance drain (2026-09-09): produce p99 4.72s
		// with a max of 5.0097s, while the BROKER's own produce handling stayed
		// flat at 0.18ms and its worst sampled window was 0.565ms. The time was
		// not spent in Redpanda at all - the record sat here, waiting to be
		// told where the new leader was.
		//
		// MetadataMinAge is the cause. It rate-limits how soon the client will
		// re-fetch metadata after a NOT_LEADER, and it defaults to 5s - which
		// is the 5.0097s max, near-exactly. The floor franz-go allows is 10ms;
		// 100ms recovers within a fraction of a leadership transfer while still
		// bounding how hard a large fleet can hammer the cluster for metadata.
		kgo.MetadataMinAge(100*time.Millisecond),

		// The retry backoff compounds it: the default escalates from 250ms to a
		// 5s cap, so a couple of failed attempts during a transfer add seconds
		// of their own on top of the metadata wait. This keeps retries inside
		// the window a leadership transfer actually takes.
		kgo.RetryBackoffFn(func(fails int) time.Duration {
			const (
				base = 50 * time.Millisecond
				max  = 500 * time.Millisecond
			)
			d := base << min(fails, 4)
			if d > max {
				return max
			}
			return d
		}),
	)
	if err != nil {
		log.Fatalf("wasm-client: creating kafka client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	h := &host{
		ctx:         ctx,
		client:      client,
		outputTopic: outputTopic,
		incoming:    make(chan []pendingRecord, 4),
	}

	// Background fetch loop - franz-go's own PollFetches, feeding
	// read_batch_header's blocking wait. This is the same long-poll
	// path any ordinary external consumer already gets - nothing
	// wasm-specific about it.
	go h.fetchLoop()

	store := wasmtime.NewStore(engine)
	wasiConfig := wasmtime.NewWasiConfig()
	wasiConfig.InheritEnv()
	wasiConfig.InheritStdout()
	wasiConfig.InheritStderr()
	store.SetWasi(wasiConfig)

	linker := wasmtime.NewLinker(engine)
	if err := linker.DefineWasi(); err != nil {
		log.Fatalf("wasm-client: DefineWasi: %v", err)
	}
	if err := h.defineHostFuncs(linker, store); err != nil {
		log.Fatalf("wasm-client: defining host funcs: %v", err)
	}
	// book-matcher imports the broker's shared-memory host module for its
	// optional snapshot state. Outside the broker there is nothing to persist
	// into, so register_region is accepted and ignored - which is exactly what
	// the guest does with STATE=off (it never calls it). Defining it is what
	// lets the SAME module run in the external arm as in the in-broker one;
	// guests that do not import it are unaffected.
	if err := linker.FuncWrap("redpanda_wasm_shared_memory", "register_region",
		func(ptr, length int32) int32 { return 0 }); err != nil {
		log.Fatalf("wasm-client: defining shared-memory stub: %v", err)
	}

	instance, err := linker.Instantiate(store, module)
	if err != nil {
		log.Fatalf("wasm-client: instantiate: %v", err)
	}
	h.memory = instance.GetExport(store, "memory").Memory()

	log.Printf(
		"wasm-client: consuming %s, producing %s, brokers=%s group=%s wasm=%s",
		inputTopic, outputTopic, brokers, groupID, wasmFile)

	start := instance.GetExport(store, "_start").Func()
	// _start runs the guest's own on_record_written loop - it blocks
	// inside read_batch_header between batches and, like the real
	// engine, never returns during ordinary operation - matching the
	// engine's own contract (transform_module.h: "the guest's exported
	// _start function is called exactly once, for the whole lifetime
	// of the instance").
	if _, err := start.Call(store); err != nil {
		log.Fatalf("wasm-client: guest exited: %v", err)
	}
}

type host struct {
	ctx         context.Context
	client      *kgo.Client
	outputTopic string
	memory      *wasmtime.Memory

	incoming chan []pendingRecord
	current  []pendingRecord // records left in the batch currently being read
}

func (h *host) fetchLoop() {
	for {
		fetches := h.client.PollFetches(h.ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("wasm-client: fetch error on %s[%d]: %v", e.Topic, e.Partition, e.Err)
			}
		}
		var batch []pendingRecord
		fetches.EachRecord(func(r *kgo.Record) {
			batch = append(batch, pendingRecord{
				offset:    r.Offset,
				timestamp: r.Timestamp.UnixMilli(),
				payload:   r.Value,
			})
		})
		if len(batch) == 0 {
			continue
		}
		h.incoming <- batch
	}
}

func (h *host) mem(store wasmtime.Storelike) []byte {
	return h.memory.UnsafeData(store)
}

func putI64(buf []byte, off int32, v int64) { binary.LittleEndian.PutUint64(buf[off:], uint64(v)) }
func putI32(buf []byte, off int32, v int32) { binary.LittleEndian.PutUint32(buf[off:], uint32(v)) }
func putI16(buf []byte, off int32, v int16) { binary.LittleEndian.PutUint16(buf[off:], uint16(v)) }

// defineHostFuncs implements the 5-function ABI any redpanda-transform-sdk
// guest imports. Semantics match src/v/wasm/transform_module.cc exactly -
// see that file's read_batch_header/read_next_record/write_record/
// write_record_with_options for the reference implementation each of
// these mirrors.
func (h *host) defineHostFuncs(linker *wasmtime.Linker, store *wasmtime.Store) error {
	const mod = "redpanda_transform"

	if err := linker.FuncWrap(mod, "check_abi_version_2", func() {
		// Real host: "does nothing at runtime, it's only an
		// opportunity for static analysis" (transform_module.cc).
	}); err != nil {
		return err
	}

	// read_batch_header(base_offset, record_count, partition_leader_epoch,
	//   attributes, last_offset_delta, base_timestamp, max_timestamp,
	//   producer_id, producer_epoch, base_sequence) -> i32
	// Blocks until a batch is available (this host's equivalent of
	// guest_wait_for_batch()), fills the header fields, and returns
	// max_input_record_size on success or noActiveTransform if there's
	// nothing more to process. This host never shuts down mid-run, so
	// in practice this only ever blocks, never returns the sentinel.
	if err := linker.FuncWrap(mod, "read_batch_header",
		func(caller *wasmtime.Caller,
			baseOffset, recordCount, partitionLeaderEpoch, attributes,
			lastOffsetDelta, baseTimestamp, maxTimestamp,
			producerID, producerEpoch, baseSequence int32,
		) int32 {
			batch := <-h.incoming
			h.current = batch
			mem := h.mem(caller)
			first := batch[0]
			putI64(mem, baseOffset, first.offset)
			putI32(mem, recordCount, int32(len(batch)))
			putI32(mem, partitionLeaderEpoch, 0) // no equivalent concept for this host
			putI16(mem, attributes, 0)
			putI32(mem, lastOffsetDelta, int32(len(batch)-1))
			putI64(mem, baseTimestamp, first.timestamp)
			putI64(mem, maxTimestamp, batch[len(batch)-1].timestamp)
			putI64(mem, producerID, -1) // no idempotent-producer identity on the read side
			putI16(mem, producerEpoch, -1)
			putI32(mem, baseSequence, -1)
			return maxInputRecordSize
		}); err != nil {
		return err
	}

	// read_next_record(attributes, timestamp, offset, buf_ptr, buf_len) -> i32
	// Pops one record from the current batch; noActiveTransform means
	// "batch exhausted, call read_batch_header again" (the same
	// sentinel value is reused for both meanings in the real host too -
	// see transform_module.cc's read_next_record).
	if err := linker.FuncWrap(mod, "read_next_record",
		func(caller *wasmtime.Caller,
			attributes, timestamp, offset, bufPtr, bufLen int32,
		) int32 {
			if len(h.current) == 0 {
				return noActiveTransform
			}
			rec := h.current[0]
			// Envelope, not the raw Kafka value - see the type comment
			// on writtenRecord for why. This project's input records
			// never carry a Kafka-level key (the order/fill ID lives
			// inside the value's own wire format), so key is always
			// None here.
			envelope := encodeRecordEnvelope(nil, rec.payload, true, false)
			if int32(len(envelope)) > bufLen {
				return invalidBuffer
			}
			h.current = h.current[1:]
			mem := h.mem(caller)
			mem[attributes] = 0
			putI64(mem, timestamp, rec.timestamp)
			putI64(mem, offset, rec.offset)
			copy(mem[bufPtr:], envelope)
			return int32(len(envelope))
		}); err != nil {
		return err
	}

	// write_record(buf_ptr, buf_len) -> i32
	// buf is the same key/value/headers envelope read_next_record
	// produces on the input side - see encodeRecordEnvelope/
	// decodeRecordEnvelope's doc comments. The return value on success
	// is the ENVELOPE's length (what the guest sent), not the decoded
	// value's length - core-sys/src/lib.rs's AbiRecordWriter checks
	// `errno_or_amt == self.output_buffer.len()`, i.e. against the
	// envelope it wrote, so echoing bufLen here (not len(value)) is
	// required, not a simplification.
	if err := linker.FuncWrap(mod, "write_record",
		func(caller *wasmtime.Caller, bufPtr, bufLen int32) int32 {
			mem := h.mem(caller)
			envelope := make([]byte, bufLen)
			copy(envelope, mem[bufPtr:bufPtr+bufLen])
			_, value, ok := decodeRecordEnvelope(envelope)
			if !ok {
				return invalidBuffer
			}
			h.produce(writtenRecord{payload: value})
			return bufLen
		}); err != nil {
		return err
	}

	// write_record_with_options(buf_ptr, buf_len, opts_ptr, opts_len) -> i32
	// Options wire format (transform_module.cc's write_options::parse):
	// a sequence of [tag:1][len:varint][bytes] entries, tag 0x01 =
	// output topic override, 0x02 = partition key. This host's
	// varint decoding is a best-effort match, not verified byte-for-
	// byte against ffi::reader's own varint implementation - neither
	// of this project's current guests (wasm-matcher, position-
	// tracker) exercise this path today, both call plain write_record.
	if err := linker.FuncWrap(mod, "write_record_with_options",
		func(caller *wasmtime.Caller, bufPtr, bufLen, optsPtr, optsLen int32) int32 {
			mem := h.mem(caller)
			envelope := make([]byte, bufLen)
			copy(envelope, mem[bufPtr:bufPtr+bufLen])
			_, value, ok := decodeRecordEnvelope(envelope)
			if !ok {
				return invalidBuffer
			}
			wr := writtenRecord{payload: value}
			opts := mem[optsPtr : optsPtr+optsLen]
			for len(opts) > 0 {
				tag := opts[0]
				opts = opts[1:]
				n, consumed := decodeVarint(opts)
				if consumed == 0 {
					return invalidBuffer
				}
				opts = opts[consumed:]
				if int(n) > len(opts) {
					return invalidBuffer
				}
				val := opts[:n]
				opts = opts[n:]
				switch tag {
				case 0x01:
					wr.topic = string(val)
				case 0x02:
					wr.partitionKey = append([]byte{}, val...)
				default:
					return invalidBuffer
				}
			}
			if wr.topic != "" && wr.topic != h.outputTopic {
				// This host only produces to the one configured
				// output topic - matches is_valid_output_topic()
				// rejecting anything outside the transform's
				// declared output-topic list.
				return invalidWrite
			}
			h.produce(wr)
			return bufLen
		}); err != nil {
		return err
	}
	return nil
}

func (h *host) produce(wr writtenRecord) {
	rec := &kgo.Record{Topic: h.outputTopic, Value: wr.payload}
	if wr.partitionKey != nil {
		rec.Key = wr.partitionKey
	}
	h.client.Produce(h.ctx, rec, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("wasm-client: produce error: %v", err)
		}
	})
}

// decodeVarint reads an unsigned LEB128 varint, returning the value and
// the number of bytes consumed (0 on error/truncation).
func decodeVarint(buf []byte) (uint64, int) {
	var v uint64
	for i, b := range buf {
		v |= uint64(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			return v, i + 1
		}
	}
	return 0, 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
