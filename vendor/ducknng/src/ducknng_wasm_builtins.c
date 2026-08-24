/*
 * Compiler-rt builtins for the Emscripten side-module build.
 *
 * duckdb-wasm side modules do not link compiler-rt, so any 128-bit helper the
 * compiler emits becomes an "env" import that must exactly match a legalized
 * export of the duckdb-wasm main module — a fragile ABI coupling that has
 * already broken extension load once (LLVM fuses the portable
 * `a > UINT64_MAX / b` overflow-check-then-multiply idiom in
 * src/ducknng_quack.c into a 128-bit multiply, importing __multi3). Defining
 * the builtin here resolves the reference inside the side module at link
 * time, so the import disappears entirely.
 *
 * The implementation is the standard compiler-rt decomposition: 64x64->128
 * via 32-bit halves, then cross terms. Only i64 arithmetic is used, which is
 * native in wasm, so this cannot recurse into itself.
 *
 * On native targets this translation unit is empty.
 */

#if defined(__EMSCRIPTEN__) && defined(__SIZEOF_INT128__)

#include <stdint.h>

/* All limb arithmetic is unsigned so the modulo-2^64 wrap the contract
 * requires is well-defined C; only the union reinterprets the final bits as
 * the signed 128-bit result. */
typedef union {
    __int128 all;
    struct {
        uint64_t low;
        uint64_t high;
    } s;
} ducknng_twords;

static __int128 ducknng_mulddi3(uint64_t a, uint64_t b) {
    ducknng_twords r;
    const uint64_t lower_mask = (uint64_t)~0ull >> 32;
    uint64_t t;

    r.s.low = (a & lower_mask) * (b & lower_mask);
    t = r.s.low >> 32;
    r.s.low &= lower_mask;
    t += (a >> 32) * (b & lower_mask);
    r.s.low += (t & lower_mask) << 32;
    r.s.high = t >> 32;
    t = r.s.low >> 32;
    r.s.low &= lower_mask;
    t += (a & lower_mask) * (b >> 32);
    r.s.low += (t & lower_mask) << 32;
    r.s.high += t >> 32;
    r.s.high += (a >> 32) * (b >> 32);
    return r.all;
}

__int128 __multi3(__int128 a, __int128 b) {
    ducknng_twords x;
    ducknng_twords y;
    ducknng_twords r;

    x.all = a;
    y.all = b;
    r.all = ducknng_mulddi3(x.s.low, y.s.low);
    r.s.high += x.s.high * y.s.low + x.s.low * y.s.high;
    return r.all;
}

#endif /* __EMSCRIPTEN__ && __SIZEOF_INT128__ */
