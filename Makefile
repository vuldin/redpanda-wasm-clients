# The guests are the product; the host is how you run one outside the broker.
#
# build-wasm produces the SAME artifact the in-broker leg deploys. Both legs of
# the published comparison run this byte-identical module - that identity is
# what makes the comparison a statement about where code executes rather than
# about two different implementations - so verify it with `make checksum` and
# record the value alongside any result.

WASM_TARGET := wasm32-wasip1
BIN         := bin

.PHONY: all build-wasm build-host checksum test clean

all: build-wasm build-host

build-wasm:
	mkdir -p $(BIN)
	cd rust && cargo build --release --target $(WASM_TARGET)
	cp rust/target/$(WASM_TARGET)/release/wasm-matcher-rs.wasm $(BIN)/wasm-matcher.wasm
	cp rust/target/$(WASM_TARGET)/release/relay-probe.wasm     $(BIN)/relay-probe.wasm
	cp rust/target/$(WASM_TARGET)/release/passthrough.wasm     $(BIN)/passthrough.wasm
	cp rust/target/$(WASM_TARGET)/release/relay-sink.wasm      $(BIN)/relay-sink.wasm
	cp rust/target/$(WASM_TARGET)/release/position-tracker.wasm $(BIN)/position-tracker.wasm

build-host:
	mkdir -p $(BIN)
	go build -o $(BIN)/wasm-client ./host

# Record this before any measurement run. A result is only comparable to
# another if both ran the same module.
checksum: 
	@md5sum $(BIN)/*.wasm

test:
	cd rust && cargo test

clean:
	rm -rf $(BIN) rust/target
