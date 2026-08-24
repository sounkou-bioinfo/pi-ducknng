#ifndef DUCKNNG_WASM_COMPAT_H
#define DUCKNNG_WASM_COMPAT_H

#ifdef __EMSCRIPTEN__

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef DUCKNNG_WASM_DUCKDB_RUNTIME
/*
 * duckdb-wasm's main module exports legalized libc entry points for some
 * 64-bit signatures and native i64 variants under orig$*.  Side modules built
 * with -sWASM_BIGINT must bind to the native variants or browser dynamic
 * linking fails with env.<symbol> signature mismatches.
 */
extern off_t ducknng_wasm_orig_lseek(int fd, off_t offset, int whence) __asm__("orig$lseek");
extern int ducknng_wasm_orig_ftruncate(int fd, off_t length) __asm__("orig$ftruncate");
extern long long ducknng_wasm_orig_strtoll(const char *nptr, char **endptr, int base) __asm__("orig$strtoll");
extern unsigned long long ducknng_wasm_orig_strtoull(const char *nptr, char **endptr, int base) __asm__("orig$strtoull");
extern long long ducknng_wasm_orig_atoll(const char *nptr) __asm__("orig$atoll");
extern time_t ducknng_wasm_orig_time(time_t *timer) __asm__("orig$time");

#define lseek ducknng_wasm_orig_lseek
#define ftruncate ducknng_wasm_orig_ftruncate
#define strtoll ducknng_wasm_orig_strtoll
#define strtoull ducknng_wasm_orig_strtoull
#define atoll ducknng_wasm_orig_atoll
#define time ducknng_wasm_orig_time
#endif

#endif

#endif
