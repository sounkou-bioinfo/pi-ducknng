#include "ducknng_checked.h"
#include <stdint.h>

int ducknng_size_add(size_t a, size_t b, size_t *out) {
    if (b > SIZE_MAX - a) return -1;
    if (out) *out = a + b;
    return 0;
}

int ducknng_size_mul(size_t a, size_t b, size_t *out) {
    if (a != 0 && b > SIZE_MAX / a) return -1;
    if (out) *out = a * b;
    return 0;
}

int ducknng_u64_add(uint64_t a, uint64_t b, uint64_t *out) {
    if (b > UINT64_MAX - a) return -1;
    if (out) *out = a + b;
    return 0;
}

int ducknng_u64_mul(uint64_t a, uint64_t b, uint64_t *out) {
    if (a != 0 && b > UINT64_MAX / a) return -1;
    if (out) *out = a * b;
    return 0;
}
