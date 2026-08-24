#pragma once
#include <stddef.h>
#include <stdint.h>

/* Checked size arithmetic. On overflow, return -1 and leave *out untouched.
 * A NULL output is permitted when the caller only needs validation. */
int ducknng_size_add(size_t a, size_t b, size_t *out);
int ducknng_size_mul(size_t a, size_t b, size_t *out);

/* The same checks at a fixed 64-bit width, for wire-derived row and element
 * counts that are uint64_t regardless of the platform's size_t. Callers that
 * then narrow to a smaller type must range-check the result themselves. */
int ducknng_u64_add(uint64_t a, uint64_t b, uint64_t *out);
int ducknng_u64_mul(uint64_t a, uint64_t b, uint64_t *out);
