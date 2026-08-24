.PHONY: clean clean_all check_news docs function_catalog rdm rpc_smoke rpc_smoke_r http_smoke subscriber_gateway_rdm

rpc_smoke: check_configure
	$(TEST_RUNNER_RELEASE)

http_smoke: release
	python3 test/http_smoke.py build/release/ducknng.duckdb_extension

subscriber_gateway_rdm: release
	R -e "rmarkdown::render('demo/subscriber_gateway.Rmd')"

check_news:
ifdef BASE
	python3 scripts/check_news.py --base "$(BASE)"
else
	python3 scripts/check_news.py
endif

docs: rdm subscriber_gateway_rdm

rpc_smoke_r:
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript test/rpc_smoke.R; \
	else \
		echo "Rscript not found; skipping optional rpc_smoke_r"; \
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
TARGET_DUCKDB_VERSION=v1.5.0

# DuckDB version used by the Python sqllogictest runner — must match TARGET_DUCKDB_VERSION
DUCKDB_TEST_VERSION=1.5.0

# Actual DuckDB release to fetch headers from for compilation
DUCKDB_HEADER_VERSION=v1.5.0

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

BASE_HEADER_URL=https://raw.githubusercontent.com/duckdb/duckdb/$(DUCKDB_HEADER_VERSION)/src/include
DUCKDB_C_HEADER_URL=$(BASE_HEADER_URL)/duckdb.h
DUCKDB_C_EXTENSION_HEADER_URL=$(BASE_HEADER_URL)/duckdb_extension.h

configure: venv platform extension_version

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

rdm: function_catalog
	R -e "rmarkdown::render('README.Rmd')"
