.PHONY: clean clean_all check_news docs function_catalog wasm_matrix rdm rpc_smoke rpc_smoke_r
.PHONY: site site-clean
.PHONY: elf-export-check
.PHONY: rpc_bench rpc_direct_bench rpc_bulk_compare rpc_upload_compare http_smoke ws_smoke subscriber_gateway_rdm
.PHONY: prop prop-quick prop-regression prop-asan prop-ubsan prop-sanitize prop-clean quack-fixtures
.PHONY: quack-coverage quack-mcdc quack-mcdc-check quack-mcdc-clean
.PHONY: fuzz fuzz-asan fuzz-ubsan fuzz-clean

rpc_smoke: check_configure
	$(TEST_RUNNER_RELEASE)

elf-export-check: release
	@symbols="$$(nm -D --defined-only build/release/libducknng.so | awk '{print $$3}')"; \
	if [ "$$symbols" != "ducknng_init_c_api" ]; then \
		echo "unexpected ELF dynamic exports:" >&2; printf '%s\n' "$$symbols" >&2; exit 1; \
	fi; \
	echo "ELF dynamic exports: ducknng_init_c_api"

http_smoke: release
	python3 test/http_smoke.py build/release/ducknng.duckdb_extension

ws_smoke: release
	python3 test/ws_smoke.py build/release/ducknng.duckdb_extension

subscriber_gateway_rdm: release
	R -e "rmarkdown::render('demo/subscriber_gateway.Rmd')"

PROP_CC ?= cc
PROP_TRIALS ?= 1000
PROP_QUICK_TRIALS ?= 200
PROP_SEED ?= 0xd17c0ffee1234567
PROP_BIN := test/bin/ducknng_prop
PROP_ASAN_BIN := test/bin/ducknng_prop_asan
PROP_UBSAN_BIN := test/bin/ducknng_prop_ubsan
PROP_DUCKNNG_SRCS := \
	src/ducknng_checked.c \
	src/ducknng_wire_core.c \
	src/ducknng_transport.c \
	src/ducknng_util.c \
	src/ducknng_quack_core.c \
	src/ducknng_quack.c
PROP_THEFT_SRCS := $(wildcard test/vendor/theft/src/*.c)
PROP_SRCS := test/property/ducknng_prop.c $(PROP_DUCKNNG_SRCS) $(PROP_THEFT_SRCS)
PROP_HDRS := \
	$(wildcard test/vendor/greatest/*.h) \
	$(wildcard test/vendor/theft/inc/*.h) \
	$(wildcard test/vendor/theft/src/*.h) \
	$(wildcard src/include/*.h) \
	$(wildcard duckdb_capi/*.h)
PROP_COMMON_CFLAGS := \
	-std=c99 -g -O1 -Wall -Wextra -Wno-unused-function -D_DEFAULT_SOURCE \
	-DDUCKDB_EXTENSION_API_VERSION_MAJOR=1 \
	-DDUCKDB_EXTENSION_API_VERSION_MINOR=5 \
	-DDUCKDB_EXTENSION_API_VERSION_PATCH=2 \
	-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=v1.5.2 \
	-DTHEFT_USE_FLOATING_POINT=0 \
	-DDUCKNNG_CORE_ONLY=1 \
	-ffunction-sections -fdata-sections \
	-Itest/vendor/greatest \
	-Itest/vendor/theft/inc \
	-Itest/vendor/theft/src \
	-Isrc/include \
	-Iduckdb_capi \
	-Ithird_party/nng/include
PROP_COMMON_LDFLAGS := -Wl,--gc-sections -pthread

$(PROP_BIN): $(PROP_SRCS) $(PROP_HDRS) Makefile | test/bin
	$(PROP_CC) $(PROP_COMMON_CFLAGS) $(PROP_CFLAGS_EXTRA) $(PROP_SRCS) \
		$(PROP_COMMON_LDFLAGS) $(PROP_LDFLAGS_EXTRA) -o $@

$(PROP_ASAN_BIN): $(PROP_SRCS) $(PROP_HDRS) Makefile | test/bin
	$(PROP_CC) $(PROP_COMMON_CFLAGS) -O1 -fsanitize=address \
		-fno-omit-frame-pointer $(PROP_SRCS) $(PROP_COMMON_LDFLAGS) \
		-fsanitize=address -o $@

$(PROP_UBSAN_BIN): $(PROP_SRCS) $(PROP_HDRS) Makefile | test/bin
	$(PROP_CC) $(PROP_COMMON_CFLAGS) -O1 -fsanitize=undefined \
		-fno-omit-frame-pointer $(PROP_SRCS) $(PROP_COMMON_LDFLAGS) \
		-fsanitize=undefined -o $@

test/bin:
	mkdir -p test/bin

prop: $(PROP_BIN)
	DUCKNNG_PROP_TRIALS=$(PROP_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) $(PROP_BIN)

prop-quick: $(PROP_BIN)
	DUCKNNG_PROP_TRIALS=$(PROP_QUICK_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) $(PROP_BIN)

prop-regression: prop

quack-fixtures:
	Rscript tools/quack_compressed_fixtures.R --check

prop-asan: $(PROP_ASAN_BIN)
	ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 \
		DUCKNNG_PROP_TRIALS=$(PROP_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) \
		$(PROP_ASAN_BIN)

prop-ubsan: $(PROP_UBSAN_BIN)
	UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1 \
		DUCKNNG_PROP_TRIALS=$(PROP_TRIALS) DUCKNNG_PROP_SEED=$(PROP_SEED) \
		$(PROP_UBSAN_BIN)

prop-sanitize: prop-asan prop-ubsan

prop-clean:
	rm -f $(PROP_BIN) $(PROP_ASAN_BIN) $(PROP_UBSAN_BIN)

MCDC_CC ?= clang
MCDC_LLVM_COV ?= $(shell command -v llvm-cov 2>/dev/null || command -v llvm-cov-18 2>/dev/null || printf '%s' /usr/lib/llvm-18/bin/llvm-cov)
MCDC_LLVM_PROFDATA ?= $(shell command -v llvm-profdata 2>/dev/null || command -v llvm-profdata-18 2>/dev/null || printf '%s' /usr/lib/llvm-18/bin/llvm-profdata)
MCDC_TRIALS ?= 2000
MCDC_BUILD_DIR := .coverage/quack-mcdc
MCDC_BIN := $(MCDC_BUILD_DIR)/ducknng_core_mcdc
MCDC_RAW_PROFILE := $(MCDC_BUILD_DIR)/default.profraw
MCDC_PROFILE := $(MCDC_BUILD_DIR)/default.profdata
MCDC_CORE_SRCS := src/ducknng_checked.c src/ducknng_wire_core.c src/ducknng_quack_core.c
MCDC_SUPPORT_SRCS := $(filter-out $(MCDC_CORE_SRCS),$(PROP_SRCS))
MCDC_CORE_OBJS := $(addprefix $(MCDC_BUILD_DIR)/,$(MCDC_CORE_SRCS:.c=.o))
MCDC_SUPPORT_OBJS := $(addprefix $(MCDC_BUILD_DIR)/,$(MCDC_SUPPORT_SRCS:.c=.o))
MCDC_OBJS := $(MCDC_CORE_OBJS) $(MCDC_SUPPORT_OBJS)
MCDC_CFLAGS := $(filter-out -O1,$(PROP_COMMON_CFLAGS)) -O0
MCDC_INSTRUMENT_FLAGS := -fprofile-instr-generate -fcoverage-mapping -fcoverage-mcdc
MCDC_LIBSTDCXX_DIR := $(shell dirname "$$(cc -print-file-name=libstdc++.so)")

$(MCDC_SUPPORT_OBJS): $(MCDC_BUILD_DIR)/%.o: %.c $(PROP_HDRS) Makefile
	@mkdir -p $(@D)
	$(MCDC_CC) $(MCDC_CFLAGS) -c $< -o $@

$(MCDC_CORE_OBJS): $(MCDC_BUILD_DIR)/%.o: %.c $(PROP_HDRS) Makefile
	@mkdir -p $(@D)
	$(MCDC_CC) $(MCDC_CFLAGS) $(MCDC_INSTRUMENT_FLAGS) -c $< -o $@

$(MCDC_BIN): $(MCDC_OBJS)
	$(MCDC_CC) $(MCDC_INSTRUMENT_FLAGS) $(MCDC_OBJS) \
		$(PROP_COMMON_LDFLAGS) -L$(MCDC_LIBSTDCXX_DIR) -lstdc++ -o $@

$(MCDC_PROFILE): $(MCDC_BIN)
	rm -f $(MCDC_RAW_PROFILE) $@
	LLVM_PROFILE_FILE=$(MCDC_RAW_PROFILE) DUCKNNG_PROP_TRIALS=$(MCDC_TRIALS) \
		DUCKNNG_PROP_SEED=$(PROP_SEED) $(MCDC_BIN) >/dev/null
	$(MCDC_LLVM_PROFDATA) merge -sparse $(MCDC_RAW_PROFILE) -o $@

quack-coverage: $(MCDC_PROFILE)
	$(MCDC_LLVM_COV) report $(MCDC_BIN) -instr-profile=$(MCDC_PROFILE) \
		--show-branch-summary $(MCDC_CORE_SRCS)

quack-mcdc: $(MCDC_PROFILE)
	$(MCDC_LLVM_COV) report $(MCDC_BIN) -instr-profile=$(MCDC_PROFILE) \
		--show-branch-summary --show-mcdc-summary $(MCDC_CORE_SRCS)

quack-mcdc-check: $(MCDC_PROFILE)
	$(MCDC_LLVM_COV) export $(MCDC_BIN) -instr-profile=$(MCDC_PROFILE) \
		--summary-only $(MCDC_CORE_SRCS) | python3 tools/check_mcdc.py

quack-mcdc-clean:
	rm -rf $(MCDC_BUILD_DIR)

FUZZ_CC ?= clang
FUZZ_RUNS ?= 100000
FUZZ_MAX_LEN ?= 65536
FUZZ_SEED ?= 3735928559
FUZZ_BUILD_DIR ?= .fuzz
FUZZ_CORPUS := test/fuzz/corpus
FUZZ_DRIVER := test/fuzz/ducknng_fuzz.c
FUZZ_DUCKNNG_SRCS := \
	src/ducknng_checked.c \
	src/ducknng_wire_core.c \
	src/ducknng_util.c \
	src/ducknng_quack_core.c \
	src/ducknng_quack.c
FUZZ_HEADERS := \
	$(wildcard src/include/*.h) \
	$(wildcard duckdb_capi/*.h)
FUZZ_COMMON_CFLAGS := \
	-std=c99 -g -O1 -Wall -Wextra -Wno-unused-function -D_DEFAULT_SOURCE \
	-DDUCKDB_EXTENSION_API_VERSION_MAJOR=1 \
	-DDUCKDB_EXTENSION_API_VERSION_MINOR=5 \
	-DDUCKDB_EXTENSION_API_VERSION_PATCH=2 \
	-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=v1.5.2 \
	-DDUCKNNG_CORE_ONLY=1 \
	-fvisibility=hidden -ffunction-sections -fdata-sections \
	-Isrc/include \
	-Iduckdb_capi \
	-Ithird_party/nng/include
FUZZ_LIBSTDCXX_DIR := $(shell dirname "$$(cc -print-file-name=libstdc++.so)")
FUZZ_COMMON_LDFLAGS := -Wl,--gc-sections -pthread -L$(FUZZ_LIBSTDCXX_DIR) -lstdc++
FUZZ_ASAN_BIN := $(FUZZ_BUILD_DIR)/ducknng_fuzz_asan
FUZZ_UBSAN_BIN := $(FUZZ_BUILD_DIR)/ducknng_fuzz_ubsan

$(FUZZ_ASAN_BIN): $(FUZZ_DRIVER) $(FUZZ_DUCKNNG_SRCS) $(FUZZ_HEADERS) Makefile
	mkdir -p $(FUZZ_BUILD_DIR)
	$(FUZZ_CC) $(FUZZ_COMMON_CFLAGS) -fsanitize=fuzzer,address \
		-fno-omit-frame-pointer $(FUZZ_DRIVER) $(FUZZ_DUCKNNG_SRCS) \
		$(FUZZ_COMMON_LDFLAGS) -fsanitize=fuzzer,address -o $@

$(FUZZ_UBSAN_BIN): $(FUZZ_DRIVER) $(FUZZ_DUCKNNG_SRCS) $(FUZZ_HEADERS) Makefile
	mkdir -p $(FUZZ_BUILD_DIR)
	$(FUZZ_CC) $(FUZZ_COMMON_CFLAGS) -fsanitize=fuzzer,undefined \
		-fno-omit-frame-pointer $(FUZZ_DRIVER) $(FUZZ_DUCKNNG_SRCS) \
		$(FUZZ_COMMON_LDFLAGS) -fsanitize=fuzzer,undefined -o $@

fuzz-asan: $(FUZZ_ASAN_BIN)
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT INT TERM; \
	cp $(FUZZ_CORPUS)/* "$$tmp"/; mkdir "$$tmp/artifacts"; \
	ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 \
	$(FUZZ_ASAN_BIN) "$$tmp" -dict=test/fuzz/ducknng.dict \
		-artifact_prefix="$$tmp/artifacts/" -max_len=$(FUZZ_MAX_LEN) \
		-timeout=5 -rss_limit_mb=1024 -seed=$(FUZZ_SEED) -runs=$(FUZZ_RUNS)

fuzz-ubsan: $(FUZZ_UBSAN_BIN)
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT INT TERM; \
	cp $(FUZZ_CORPUS)/* "$$tmp"/; mkdir "$$tmp/artifacts"; \
	UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1 \
	$(FUZZ_UBSAN_BIN) "$$tmp" -dict=test/fuzz/ducknng.dict \
		-artifact_prefix="$$tmp/artifacts/" -max_len=$(FUZZ_MAX_LEN) \
		-timeout=5 -rss_limit_mb=1024 -seed=$(FUZZ_SEED) -runs=$(FUZZ_RUNS)

fuzz: fuzz-asan fuzz-ubsan

fuzz-clean:
	rm -rf $(FUZZ_BUILD_DIR)

check_news:
ifdef BASE
	python3 scripts/check_news.py --base "$(BASE)"
else
	python3 scripts/check_news.py
endif

docs: rdm subscriber_gateway_rdm

# Curated documentation site. Renders README.md plus the seven published
# contracts from docs/ into _site/ with litedown; .github/workflows/pages.yml
# publishes that directory to the gh-pages root.
site:
	Rscript scripts/build_docs_site.R

site-clean:
	rm -rf _site

rpc_smoke_r:
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript test/rpc_smoke.R; \
	else \
		echo "Rscript not found; skipping optional rpc_smoke_r"; \
	fi

rpc_bench: release
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript bench/rpc_quack_bench.R; \
	else \
		echo "Rscript not found; skipping optional rpc_bench"; \
	fi

rpc_direct_bench: release
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript bench/rpc_quack_bench.R direct; \
	else \
		echo "Rscript not found; skipping optional rpc_direct_bench"; \
	fi

rpc_bulk_compare: release
	@if command -v R >/dev/null 2>&1; then \
		R -e "rmarkdown::render('bench/rpc_bulk_compare.Rmd')"; \
	else \
		echo "R not found; skipping optional rpc_bulk_compare"; \
	fi

rpc_upload_compare: release
	@if command -v R >/dev/null 2>&1; then \
		DUCKNNG_BENCH_EXT_PATH="$${DUCKNNG_BENCH_EXT_PATH:-$(CURDIR)/build/release/ducknng.duckdb_extension}" \
		R -e "rmarkdown::render('bench/rpc_upload_compare.Rmd')"; \
	else \
		echo "R not found; skipping optional rpc_upload_compare"; \
	fi

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=ducknng
# USE_UNSTABLE_C_API=1 is required to access the DuckDB Arrow conversion API
# (duckdb_to_arrow_schema, duckdb_data_chunk_to_arrow, duckdb_schema_from_arrow,
# duckdb_data_chunk_from_arrow) and the error-data API (duckdb_create_error_data,
# duckdb_error_data_message, duckdb_error_data_has_error). These functions live
# behind DUCKDB_EXTENSION_API_VERSION_UNSTABLE in the extension vtable and are
# used in src/ducknng_ipc_out.c to replace ~530 lines of hand-written per-type
# Arrow encoding with correct, DuckDB-maintained conversions.
USE_UNSTABLE_C_API=1

# Target DuckDB version — must match DUCKDB_HEADER_VERSION so the unstable
# vtable layout seen at compile time matches the runtime vtable provided by the
# host.  Both the Python sqllogictest runner and the R duckdb package in use
# are v1.5.2, so all three version pins are kept in sync here.
TARGET_DUCKDB_VERSION=v1.5.2

# DuckDB version used by the Python sqllogictest runner — must match TARGET_DUCKDB_VERSION
DUCKDB_TEST_VERSION=1.5.2

# Actual DuckDB release to fetch headers from for compilation
DUCKDB_HEADER_VERSION=v1.5.2

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

BASE_HEADER_URL=https://raw.githubusercontent.com/duckdb/duckdb/$(DUCKDB_HEADER_VERSION)/src/include
DUCKDB_C_HEADER_URL=$(BASE_HEADER_URL)/duckdb.h
DUCKDB_C_EXTENSION_HEADER_URL=$(BASE_HEADER_URL)/duckdb_extension.h

configure: venv platform extension_version

.PHONY: refresh_extension_version
refresh_extension_version:
	@$(VERSION_COMMAND)

build_extension_with_metadata_debug: refresh_extension_version
build_extension_with_metadata_release: refresh_extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

# duckdb-wasm side modules need to match the runtime ABI.  The EH and
# threads runtimes use native wasm exceptions and native i64/BigInt imports;
# pthread builds also need the Emscripten link-time tuning used by nanonext.
ifneq ($(DUCKDB_WASM_PLATFORM),)
CMAKE_EXTRA_BUILD_FLAGS += -DDUCKNNG_DUCKDB_WASM_PLATFORM=$(DUCKDB_WASM_PLATFORM)
ifneq ($(DUCKNNG_WASM_TRACE),)
CMAKE_EXTRA_BUILD_FLAGS += -DDUCKNNG_WASM_TRACE=$(DUCKNNG_WASM_TRACE)
endif
ifneq ($(DUCKNNG_WASM_INPROC_ONLY),)
CMAKE_EXTRA_BUILD_FLAGS += -DDUCKNNG_WASM_INPROC_ONLY=$(DUCKNNG_WASM_INPROC_ONLY)
endif
DUCKNNG_WASM_LINK_FLAGS := -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="_$(EXTENSION_NAME)_init_c_api"
ifeq ($(DUCKDB_WASM_PLATFORM),wasm_eh)
DUCKNNG_WASM_LINK_FLAGS += -sWASM_BIGINT -fwasm-exceptions
endif
ifeq ($(DUCKDB_WASM_PLATFORM),wasm_threads)
DUCKNNG_WASM_LINK_FLAGS += \
	-sWASM_BIGINT -fwasm-exceptions -pthread \
	-s PTHREAD_POOL_SIZE=16 \
	-s ALLOW_MEMORY_GROWTH=1 \
	-s INITIAL_MEMORY=33554432
endif
link_wasm_debug:
	@WASM_LINK_RSP=""; \
	if [ -f "cmake_build/debug/wasm_link_inputs.rsp" ]; then \
		WASM_LINK_RSP="@cmake_build/debug/wasm_link_inputs.rsp"; \
	elif [ -f "$(EXTENSION_BUILD_PATH)/debug/wasm_link_inputs.rsp" ]; then \
		WASM_LINK_RSP="@$(EXTENSION_BUILD_PATH)/debug/wasm_link_inputs.rsp"; \
	else \
		echo "ducknng wasm dependency response file not found" >&2; \
		exit 1; \
	fi; \
	emcc $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME) $$WASM_LINK_RSP \
		-o $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_FILENAME_NO_METADATA) \
		-O3 -g $(DUCKNNG_WASM_LINK_FLAGS)

link_wasm_release:
	@WASM_LINK_RSP=""; \
	if [ -f "cmake_build/release/wasm_link_inputs.rsp" ]; then \
		WASM_LINK_RSP="@cmake_build/release/wasm_link_inputs.rsp"; \
	elif [ -f "$(EXTENSION_BUILD_PATH)/release/wasm_link_inputs.rsp" ]; then \
		WASM_LINK_RSP="@$(EXTENSION_BUILD_PATH)/release/wasm_link_inputs.rsp"; \
	else \
		echo "ducknng wasm dependency response file not found" >&2; \
		exit 1; \
	fi; \
	emcc $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME) $$WASM_LINK_RSP \
		-o $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME_NO_METADATA) \
		-O3 $(DUCKNNG_WASM_LINK_FLAGS)
endif

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean clean_configure

function_catalog:
	python3 function_catalog/generate_function_catalog.py

# The matrix always renders from a fresh native build: under
# DUCKDB_PLATFORM=wasm_* `release` produces a wasm artifact the generator
# cannot load, so the platform is unset for the prerequisite build.
wasm_matrix:
	env -u DUCKDB_PLATFORM $(MAKE) release
	./configure/venv/bin/python3 scripts/generate_wasm_matrix.py

rdm: function_catalog
	R -e "rmarkdown::render('README.Rmd')"
