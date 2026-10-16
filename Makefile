# The guests are the product; the host is how you run one outside the broker.
#
# build-wasm produces the SAME artifact the in-broker leg deploys. Both legs of
# the published comparison run this byte-identical module - that identity is
# what makes the comparison a statement about where code executes rather than
# about two different implementations - so verify it with `make checksum` and
# record the value alongside any result.

WASM_TARGET := wasm32-wasip1
BIN         := bin

.PHONY: all build-wasm build-host build-host-native checksum test clean \
        restore-published verify-published

all: build-wasm build-host

build-wasm:
	mkdir -p $(BIN)
	cd rust && cargo build --release --target $(WASM_TARGET)
	cp rust/target/$(WASM_TARGET)/release/wasm-matcher-rs.wasm $(BIN)/wasm-matcher.wasm
	cp rust/target/$(WASM_TARGET)/release/relay-probe.wasm     $(BIN)/relay-probe.wasm
	cp rust/target/$(WASM_TARGET)/release/passthrough.wasm     $(BIN)/passthrough.wasm
	cp rust/target/$(WASM_TARGET)/release/relay-sink.wasm      $(BIN)/relay-sink.wasm
	cp rust/target/$(WASM_TARGET)/release/position-tracker.wasm $(BIN)/position-tracker.wasm
	cp rust/target/$(WASM_TARGET)/release/mass-matcher.wasm     $(BIN)/mass-matcher.wasm
	cp rust/target/$(WASM_TARGET)/release/book-matcher.wasm     $(BIN)/book-matcher.wasm
	cp rust/target/$(WASM_TARGET)/release/gateway-demux.wasm    $(BIN)/gateway-demux.wasm
	@# The freshly built single-order matcher is kept, but NOT under the name
	@# every runner deploys. Before this, `make build-wasm` silently replaced
	@# the published module (94c3d017) with whatever the current source
	@# compiles to (2cf80fdc as of the mass-quote work), and every remediation
	@# hint in the bench repo said to run exactly this command. A baseline run
	@# after that would have measured a different module under the published
	@# name. Use bin/wasm-matcher-current.wasm deliberately when you mean to
	@# measure a matcher change.
	cp rust/target/$(WASM_TARGET)/release/wasm-matcher-rs.wasm $(BIN)/wasm-matcher-current.wasm
	@$(MAKE) --no-print-directory restore-published
	@$(MAKE) --no-print-directory verify-published

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

PUBLISHED := rust/testdata/published-modules

# The modules that published figures were measured with, by md5.
#
# bin/ is a build output and *.wasm is gitignored, so until these were checked
# in the module behind a published number lived in exactly one place - an
# untracked working copy, one `make build-wasm` away from being gone for good.
# A rebuild from current source does NOT reproduce them (toolchain drift alone
# would see to that), so they are provenance, not artifacts.
#
# restore-published puts them back into bin/ so a baseline arm deploys the real
# thing rather than a lookalike. Run it before any run whose point is to
# reproduce a published figure, and `make checksum` after, to prove it.
restore-published:
	mkdir -p $(BIN)
	cp $(PUBLISHED)/wasm-matcher-94c3d017.wasm     $(BIN)/wasm-matcher.wasm
	cp $(PUBLISHED)/relay-probe-d722422b.wasm      $(BIN)/relay-probe.wasm
	cp $(PUBLISHED)/relay-sink-e5ba8e5f.wasm       $(BIN)/relay-sink.wasm
	cp $(PUBLISHED)/passthrough-5ac9883c.wasm      $(BIN)/passthrough.wasm
	cp $(PUBLISHED)/position-tracker-9095a58a.wasm $(BIN)/position-tracker.wasm
	@echo "restored; verify with: make verify-published"

# Fails loudly if what is in bin/ is not what published the numbers. Use it to
# tell "this is the published module" from "this is a fresh build of the same
# source", which no amount of reading the source can distinguish.
verify-published:
	@fail=0; \
	for f in $(PUBLISHED)/*.wasm; do \
	  want=$$(basename $$f | sed 's/.*-\([0-9a-f]\{8\}\)\.wasm/\1/'); \
	  name=$$(basename $$f | sed 's/-[0-9a-f]\{8\}\.wasm//'); \
	  case $$name in wasm-matcher) target=$(BIN)/wasm-matcher.wasm ;; \
	                 *) target=$(BIN)/$$name.wasm ;; esac; \
	  if [ ! -f $$target ]; then echo "MISSING  $$target"; fail=1; continue; fi; \
	  got=$$(md5sum $$target | cut -c1-8); \
	  if [ "$$got" = "$$want" ]; then echo "ok       $$target ($$got)"; \
	  else echo "DIFFERS  $$target (have $$got, published $$want)"; fail=1; fi; \
	done; \
	exit $$fail

test:
	cd rust && cargo test

clean:
	rm -rf $(BIN) rust/target
