cmake_minimum_required(VERSION 3.16...3.29)
option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)
option(DUCKNNG_WASM_TRACE "Emit diagnostic trace messages from wasm transport probes" OFF)
option(DUCKNNG_WASM_INPROC_ONLY "Limit NNG's browser wasm transport footprint to inproc" ON)

if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})
if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

project(${EXTENSION_NAME} LANGUAGES C)
set(DUCKNNG_DUCKDB_CAPI_DIR "${CMAKE_SOURCE_DIR}/duckdb_capi" CACHE PATH
    "Directory containing the exact DuckDB C extension headers")
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(USE_STATIC_MBEDTLS_LIBRARY ON CACHE BOOL "" FORCE)
set(USE_SHARED_MBEDTLS_LIBRARY OFF CACHE BOOL "" FORCE)
set(INSTALL_MBEDTLS_HEADERS OFF CACHE BOOL "" FORCE)
set(DISABLE_PACKAGE_CONFIG_AND_INSTALL ON CACHE BOOL "" FORCE)

add_subdirectory(third_party/mbedtls EXCLUDE_FROM_ALL)

set(NNG_DISABLE_PACKAGE_CONFIG_AND_INSTALL ON CACHE BOOL "" FORCE)
set(NNG_TESTS OFF CACHE BOOL "" FORCE)
set(NNG_TOOLS OFF CACHE BOOL "" FORCE)
set(NNG_ENABLE_HTTP ON CACHE BOOL "" FORCE)
set(NNG_ENABLE_TLS ON CACHE BOOL "" FORCE)
set(NNG_TLS_ENGINE "mbed" CACHE STRING "" FORCE)
set(NNG_TRANSPORT_INPROC ON CACHE BOOL "" FORCE)
if (DUCKDB_WASM_EXTENSION AND DUCKNNG_WASM_INPROC_ONLY)
    set(NNG_TRANSPORT_IPC OFF CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_TCP OFF CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_TLS OFF CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_WS OFF CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_WSS OFF CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_FDC OFF CACHE BOOL "" FORCE)
else()
    set(NNG_TRANSPORT_IPC ON CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_TCP ON CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_TLS ON CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_WS ON CACHE BOOL "" FORCE)
    set(NNG_TRANSPORT_WSS ON CACHE BOOL "" FORCE)
endif()
set(NNG_PROTO_BUS0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PAIR0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PAIR1 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PUSH0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PULL0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_PUB0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_SUB0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_REQ0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_REP0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_RESPONDENT0 ON CACHE BOOL "" FORCE)
set(NNG_PROTO_SURVEYOR0 ON CACHE BOOL "" FORCE)

add_subdirectory(third_party/nng EXCLUDE_FROM_ALL)

if (DUCKDB_WASM_EXTENSION)
    foreach(DUCKNNG_WASM_DEP_TARGET nng mbedtls mbedx509 mbedcrypto)
        if (TARGET ${DUCKNNG_WASM_DEP_TARGET})
            target_compile_definitions(${DUCKNNG_WASM_DEP_TARGET} PRIVATE
                DUCKNNG_WASM_DUCKDB_RUNTIME=1)
            if (DUCKNNG_WASM_INPROC_ONLY)
                target_compile_definitions(${DUCKNNG_WASM_DEP_TARGET} PRIVATE
                    DUCKNNG_WASM_INPROC_ONLY=1)
            endif()
            if (DUCKNNG_WASM_TRACE)
                target_compile_definitions(${DUCKNNG_WASM_DEP_TARGET} PRIVATE
                    DUCKNNG_WASM_TRACE=1)
            endif()
            target_compile_options(${DUCKNNG_WASM_DEP_TARGET} PRIVATE
                -fwasm-exceptions
                -include ${CMAKE_SOURCE_DIR}/src/include/ducknng_wasm_compat.h)
            if (DUCKNNG_DUCKDB_WASM_PLATFORM STREQUAL "wasm_threads")
                target_compile_options(${DUCKNNG_WASM_DEP_TARGET} PRIVATE -pthread)
            endif()
        endif()
    endforeach()
endif()

set(EXTENSION_SOURCES
    src/ducknng_extension.c
    src/ducknng_sql_api.c
    src/ducknng_sql_auth.c
    src/ducknng_sql_monitor.c
    src/ducknng_sql_service.c
    src/ducknng_sql_http.c
    src/ducknng_sql_http_profile.c
    src/ducknng_sql_tls.c
    src/ducknng_sql_socket.c
    src/ducknng_sql_aio.c
    src/ducknng_sql_registry.c
    src/ducknng_sql_session.c
    src/ducknng_sql_body.c
    src/ducknng_sql_arrow.c
    src/ducknng_sql_rpc.c
    src/ducknng_runtime.c
    src/ducknng_service.c
    src/ducknng_session.c
    src/ducknng_duckdb_streaming_compat.c
    src/ducknng_registry.c
    src/ducknng_methods.c
    src/ducknng_checked.c
    src/ducknng_wire_core.c
    src/ducknng_wire.c
    src/ducknng_ipc_in.c
    src/ducknng_ipc_out.c
    src/ducknng_quack_core.c
    src/ducknng_quack.c
    src/ducknng_transport.c
    src/ducknng_http_profile.c
    src/ducknng_http_compat.c
    src/ducknng_http_client_stream.c
    src/ducknng_ws_frame.c
    src/ducknng_net_backend.c
    src/ducknng_wasm_http_fetch.c
    src/ducknng_wasm_builtins.c
    src/ducknng_wasm_fetch_bridge.c
    src/ducknng_wasm_ws_bridge.c
    src/ducknng_nng_compat.c
    src/ducknng_util.c
    third_party/nanoarrow/src/nanoarrow/common/array.c
    third_party/nanoarrow/src/nanoarrow/common/schema.c
    third_party/nanoarrow/src/nanoarrow/common/array_stream.c
    third_party/nanoarrow/src/nanoarrow/common/utils.c
    third_party/nanoarrow/src/nanoarrow/ipc/codecs.c
    third_party/nanoarrow/src/nanoarrow/ipc/decoder.c
    third_party/nanoarrow/src/nanoarrow/ipc/encoder.c
    third_party/nanoarrow/src/nanoarrow/ipc/reader.c
    third_party/nanoarrow/src/nanoarrow/ipc/writer.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/builder.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/emitter.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/verifier.c
    third_party/nanoarrow/thirdparty/flatcc/src/runtime/refmap.c
)

if (DUCKDB_WASM_EXTENSION)
    add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
    target_compile_definitions(${EXTENSION_NAME} PRIVATE
        DUCKNNG_WASM_DUCKDB_RUNTIME=1)
    if (DUCKNNG_WASM_INPROC_ONLY)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE
            DUCKNNG_WASM_INPROC_ONLY=1)
    endif()
    if (DUCKNNG_DUCKDB_WASM_PLATFORM STREQUAL "wasm_eh" OR
        DUCKNNG_DUCKDB_WASM_PLATFORM STREQUAL "wasm_threads")
        target_compile_options(${EXTENSION_NAME} PRIVATE
            -fwasm-exceptions
            -include ${CMAKE_SOURCE_DIR}/src/include/ducknng_wasm_compat.h)
    endif()
    if (DUCKNNG_DUCKDB_WASM_PLATFORM STREQUAL "wasm_threads")
        target_compile_options(${EXTENSION_NAME} PRIVATE -pthread)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE
            DUCKNNG_WASM_THREADS_RUNTIME=1)
    endif()
else()
    add_library(${EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})
endif()

set_target_properties(${EXTENSION_NAME} PROPERTIES
    C_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
)
if (NOT DUCKDB_WASM_EXTENSION)
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")
endif()

target_include_directories(${EXTENSION_NAME} PRIVATE
    src/include
    "${DUCKNNG_DUCKDB_CAPI_DIR}"
    third_party/nng/include
    third_party/nanoarrow/src
    third_party/nanoarrow/thirdparty/flatcc/include
)

target_compile_definitions(${EXTENSION_NAME} PRIVATE
    NANOARROW_NAMESPACE=DucknngNanoarrow
    # Bound the vendored flatcc flatbuffer verifier's recursion budget. The
    # verifier's work on a maliciously/accidentally deep nested Arrow schema is
    # exponential in the remaining ttl, so the upstream default of 100 lets a
    # ~2KB depth-48 LIST<...> schema spin effectively forever during decode
    # verification. Capping at 40 keeps legitimate nesting (depth up to ~16)
    # verifying instantly while turning anything deeper into a bounded (~8 ms)
    # clean "max nesting level reached" error instead of a hang. Producers also
    # reject over-deep results early (ducknng_ipc_out.c); this is the decode-side
    # backstop for untrusted peers. See docs/types.md.
    FLATCC_VERIFIER_MAX_LEVELS=40
)

find_package(Threads REQUIRED)
if (DUCKDB_WASM_EXTENSION AND DUCKNNG_WASM_TRACE)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCKNNG_WASM_TRACE=1)
endif()

if (DUCKDB_WASM_EXTENSION)
    target_link_libraries(${EXTENSION_NAME} PRIVATE Threads::Threads)
    add_dependencies(${EXTENSION_NAME} nng mbedtls mbedx509 mbedcrypto)
    file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/wasm_link_inputs.rsp" CONTENT
        "$<TARGET_FILE:nng>\n$<TARGET_FILE:mbedtls>\n$<TARGET_FILE:mbedx509>\n$<TARGET_FILE:mbedcrypto>\n")
else()
    target_link_libraries(${EXTENSION_NAME} PRIVATE
        $<LINK_LIBRARY:WHOLE_ARCHIVE,nng>
        Threads::Threads)
    if(APPLE)
        target_link_options(${EXTENSION_NAME} PRIVATE
            "-Wl,-exported_symbol,_ducknng_init_c_api")
    elseif(UNIX)
        set(DUCKNNG_EXPORT_MAP "${CMAKE_SOURCE_DIR}/cmake/ducknng.exports")
        target_link_options(${EXTENSION_NAME} PRIVATE
            "-Wl,--version-script=${DUCKNNG_EXPORT_MAP}"
            "-Wl,--exclude-libs,ALL"
            "-Wl,-Bsymbolic-functions")
        set_property(TARGET ${EXTENSION_NAME} APPEND PROPERTY
            LINK_DEPENDS "${DUCKNNG_EXPORT_MAP}")
    endif()
endif()
