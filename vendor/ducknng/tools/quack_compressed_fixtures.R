#!/usr/bin/env Rscript

# Generate the hand-built DuckDB BinarySerializer fixtures used by
# test/sql/ducknng_body_codecs.test. The field layout is pinned to DuckDB v1.5.2
# Vector::Serialize and DataChunk::Serialize. This script has no package
# dependencies; run it from the repository root with Rscript.

field_end <- 0xffffL

raw_join <- function(...) {
  parts <- unlist(list(...), recursive = TRUE, use.names = FALSE)
  if (!length(parts)) return(raw())
  as.raw(parts)
}

u16_le <- function(value) {
  writeBin(as.integer(value), raw(), size = 2L, endian = "little")
}

u32_le <- function(values) {
  writeBin(as.integer(values), raw(), size = 4L, endian = "little")
}

# The fixtures need only small signed 64-bit values. Writing low/high words
# avoids a bit64 dependency and preserves the integer storage representation.
i64_le <- function(value) {
  stopifnot(length(value) == 1L, is.finite(value), value == trunc(value))
  low <- as.integer(value)
  high <- if (value < 0) -1L else 0L
  raw_join(u32_le(low), u32_le(high))
}

uleb128 <- function(value) {
  stopifnot(length(value) == 1L, is.finite(value), value >= 0, value == trunc(value))
  out <- raw()
  repeat {
    byte <- value %% 128
    value <- floor(value / 128)
    if (value > 0) byte <- byte + 128
    out <- raw_join(out, as.raw(byte))
    if (value == 0) return(out)
  }
}

sleb128 <- function(value) {
  stopifnot(length(value) == 1L, is.finite(value), value == trunc(value))
  out <- raw()
  repeat {
    byte <- bitwAnd(as.integer(value), 0x7fL)
    next_value <- floor(value / 128)
    sign <- bitwAnd(byte, 0x40L) != 0L
    more <- !((next_value == 0 && !sign) || (next_value == -1 && sign))
    if (more) byte <- bitwOr(byte, 0x80L)
    out <- raw_join(out, as.raw(byte))
    value <- next_value
    if (!more) return(out)
  }
}

blob <- function(value) raw_join(uleb128(length(value)), value)
end <- function() u16_le(field_end)

logical_type <- function(id) raw_join(u16_le(100L), uleb128(id), end())

list_type <- function(child) {
  raw_join(
    u16_le(100L), uleb128(101L),
    u16_le(101L), as.raw(1L),
    u16_le(100L), uleb128(4L),
    u16_le(200L), child,
    end(), end()
  )
}

flat_i64 <- function(value, valid = TRUE) {
  validity <- if (isTRUE(valid)) {
    raw_join(u16_le(100L), as.raw(0L))
  } else {
    raw_join(u16_le(100L), as.raw(1L), u16_le(101L), blob(raw(8L)))
  }
  raw_join(validity, u16_le(102L), blob(i64_le(value)), end())
}

flat_i32 <- function(values) {
  raw_join(
    u16_le(100L), as.raw(0L),
    u16_le(102L), blob(u32_le(values)),
    end()
  )
}

flat_strings <- function(values) {
  encoded <- lapply(enc2utf8(values), function(value) blob(charToRaw(value)))
  raw_join(u16_le(100L), as.raw(0L), u16_le(102L), uleb128(length(values)), encoded, end())
}

list_entry <- function(offset, length) {
  raw_join(u16_le(100L), uleb128(offset), u16_le(101L), uleb128(length), end())
}

flat_integer_lists <- function() {
  raw_join(
    u16_le(100L), as.raw(0L),
    u16_le(104L), uleb128(4L),
    u16_le(105L), uleb128(3L),
    list_entry(0L, 2L), list_entry(2L, 0L), list_entry(2L, 2L),
    u16_le(106L), flat_i32(c(10L, 11L, 12L, 13L)),
    end()
  )
}

quack_batch <- function(result_type, name, rows, vector, chunk_type = result_type) {
  raw_join(
    u16_le(1L), uleb128(1L), result_type,
    u16_le(2L), uleb128(1L), blob(charToRaw(enc2utf8(name))),
    u16_le(4L), uleb128(1L),
    as.raw(1L), u16_le(300L),
    u16_le(100L), uleb128(rows),
    u16_le(101L), uleb128(1L), chunk_type,
    u16_le(102L), uleb128(1L), vector,
    end(), # DataChunk
    end(), # DataChunkWrapper
    end()  # batch container
  )
}

constant_vector <- function(value, valid = TRUE) {
  raw_join(u16_le(90L), uleb128(2L), flat_i64(value, valid))
}

dictionary_vector <- function(selection, values) {
  raw_join(
    u16_le(90L), uleb128(3L),
    u16_le(91L), blob(u32_le(selection)),
    u16_le(92L), uleb128(length(values)),
    flat_strings(values)
  )
}

dictionary_list_vector <- function(selection) {
  raw_join(
    u16_le(90L), uleb128(3L),
    u16_le(91L), blob(u32_le(selection)),
    u16_le(92L), uleb128(3L),
    flat_integer_lists()
  )
}

flat_integer_list_sequence <- function(child_size) {
  raw_join(
    u16_le(100L), as.raw(0L),
    u16_le(104L), uleb128(child_size),
    u16_le(105L), uleb128(1L), list_entry(0L, child_size),
    u16_le(106L), sequence_vector(0L, 1L),
    end()
  )
}

sequence_vector <- function(start, increment) {
  raw_join(
    u16_le(90L), uleb128(4L),
    u16_le(91L), sleb128(start),
    u16_le(92L), sleb128(increment),
    end()
  )
}

legacy_chunk <- function(value) {
  raw_join(
    as.raw(1L), u16_le(300L),
    u16_le(100L), uleb128(1L),
    u16_le(102L), uleb128(1L),
    flat_i64(value),
    end() # DataChunk; ducknng 0.1.2 omitted the wrapper end here
  )
}

legacy_two_chunk_batch <- function(type, name) {
  raw_join(
    u16_le(1L), uleb128(1L), type,
    u16_le(2L), uleb128(1L), blob(charToRaw(enc2utf8(name))),
    u16_le(4L), uleb128(2L),
    legacy_chunk(1L), legacy_chunk(2L),
    end(), # historical one-off terminator after the complete results list
    end()
  )
}

tinyint_type <- logical_type(11L)
bigint_type <- logical_type(14L)
uinteger_type <- logical_type(30L)
varchar_type <- logical_type(25L)
integer_list_type <- list_type(logical_type(13L))

fixtures <- list(
  constant = quack_batch(bigint_type, "v", 4L, constant_vector(42L)),
  constant_null = quack_batch(bigint_type, "v", 3L, constant_vector(0L, FALSE)),
  dictionary = quack_batch(
    varchar_type, "v", 5L,
    dictionary_vector(c(2L, 0L, 2L, 1L, 0L), c("a", "b", "c"))
  ),
  dictionary_list = quack_batch(
    integer_list_type, "v", 4L,
    dictionary_list_vector(c(2L, 0L, 1L, 2L))
  ),
  sequence = quack_batch(bigint_type, "v", 4L, sequence_vector(10L, -2L)),
  sequence_uinteger = quack_batch(uinteger_type, "v", 3L, sequence_vector(3L, 2L)),
  bad_sequence_range = quack_batch(tinyint_type, "v", 2L, sequence_vector(127L, 1L)),
  fsst_unsupported = quack_batch(
    varchar_type, "v", 1L, raw_join(u16_le(90L), uleb128(1L), end())
  ),
  bad_dictionary_index = quack_batch(
    varchar_type, "v", 5L,
    dictionary_vector(c(3L, 0L, 2L, 1L, 0L), c("a", "b", "c"))
  ),
  bad_dictionary_count = quack_batch(
    varchar_type, "v", 5L,
    dictionary_vector(c(0L, 1L, 2L, 3L, 4L), letters[1:6])
  ),
  oversized_sequence_list = quack_batch(
    integer_list_type, "v", 1L, flat_integer_list_sequence(2^22 + 1L)
  ),
  bad_chunk_type = quack_batch(
    bigint_type, "v", 4L, constant_vector(42L), logical_type(13L)
  ),
  legacy_two_chunk = legacy_two_chunk_batch(bigint_type, "v")
)

hex <- function(value) paste(sprintf("%02x", as.integer(value)), collapse = "")

args <- commandArgs(trailingOnly = TRUE)
if (identical(args, "--check")) {
  sql <- paste(readLines("test/sql/ducknng_body_codecs.test", warn = FALSE), collapse = "\n")
  missing <- names(fixtures)[!vapply(
    fixtures,
    function(value) grepl(hex(value), sql, fixed = TRUE),
    logical(1)
  )]
  if (length(missing)) {
    stop("compressed Quack fixture literals are missing or stale: ", paste(missing, collapse = ", "))
  }
  message("quack compressed fixtures: SQL literals current")
} else {
  for (name in names(fixtures)) cat(name, "\t", hex(fixtures[[name]]), "\n", sep = "")
}
