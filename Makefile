# The guests are the product; the host is how you run one outside the broker.
#
# build-wasm produces the SAME artifact the in-broker leg deploys. Both legs of
# the published comparison run this byte-identical module - that identity is
# what makes the comparison a statement about where code executes rather than
# about two different implementations - so verify it with `make checksum` and
# record the value alongside any result.

WASM_TARGET := wasm32-wasip1
BIN         := bin

.PHONY: all build-wasm build-host build-host-native checksum test clean

all: build-wasm build-host

build-wasm:
	mkdir -p $(BIN)
	cd rust && cargo build --release --target $(WASM_TARGET)
	cp rust/target/$(WASM_TARGET)/release/wasm-matcher-rs.wasm $(BIN)/wasm-matcher.wasm
	cp rust/target/$(WASM_TARGET)/release/relay-probe.wasm     $(BIN)/relay-probe.wasm
	cp rust/target/$(WASM_TARGET)/release/passthrough.wasm     $(BIN)/passthrough.wasm
	cp rust/target/$(WASM_TARGET)/release/relay-sink.wasm      $(BIN)/relay-sink.wasm
	cp rust/target/$(WASM_TARGET)/release/position-tracker.wasm $(BIN)/position-tracker.wasm

# Built inside ubuntu:22.04, NOT on the host, and that is not optional.
#
# The host embeds wasmtime via cgo, so the binary is dynamically linked against
# glibc. Built on a modern workstation it requires a glibc newer than the
# benchmark client instances provide, and fails at exec with
#   ./wasm-client: /lib/x86_64-linux-gnu/libm.so.6: version `GLIBC_2.38' not found
# which is discovered on the remote host, mid-run, against already-billing
# hardware. Building against the oldest glibc we need to support is the fix;
# CGO_ENABLED=0 is not an option because wasmtime-go needs cgo.
#
# Override GO_VERSION or UBUNTU_IMAGE if your target instances differ.
UBUNTU_IMAGE ?= ubuntu:22.04
GO_VERSION   ?= 1.24.3

build-host:
	mkdir -p $(BIN)
	docker run --rm -v "$$(pwd):/src" -w /src $(UBUNTU_IMAGE) bash -c '\
	  apt-get update -qq >/dev/null && apt-get install -y -qq gcc curl >/dev/null && \
	  curl -sL https://go.dev/dl/go$(GO_VERSION).linux-amd64.tar.gz -o /tmp/go.tar.gz && \
	  tar -C /usr/local -xzf /tmp/go.tar.gz && \
	  PATH=/usr/local/go/bin:$$PATH CGO_ENABLED=1 go build -o $(BIN)/wasm-client ./host'
	@echo "host glibc requirement:"
	@objdump -T $(BIN)/wasm-client 2>/dev/null | grep -oE 'GLIBC_[0-9.]+' | sort -Vu | tail -3

build-host-native:
	mkdir -p $(BIN)
	go build -o $(BIN)/wasm-client ./host
	@echo "WARNING: built against this machine's glibc. Fine for local use;"
	@echo "         will likely fail to exec on the benchmark instances."

# Record this before any measurement run. A result is only comparable to
# another if both ran the same module.
checksum: 
	@md5sum $(BIN)/*.wasm

test:
	cd rust && cargo test

clean:
	rm -rf $(BIN) rust/target
