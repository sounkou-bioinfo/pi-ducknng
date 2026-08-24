# Programmatically derive the unstable DuckDB C extension ABI entries that
# ducknng source code currently calls.
#
# The DuckDB extension vtable (duckdb_extension.h) annotates each group of
# function pointers with a "// Version <group>" comment.  Groups whose name
# starts with "unstable" are not part of the v1.0 stable ABI contract and
# require USE_UNSTABLE_C_API=1 at build time.  This script cross-references
# those entries against the ducknng C sources so the current unstable surface
# is always visible and auditable.
#
# Usage (from the repo root):
#   source("tools/used_duckdb_unstable_api.R")
#   ducknng_used_duckdb_unstable_api()          # per-function-per-file table
#   ducknng_used_duckdb_unstable_api_summary()  # grouped summary
#   cat(ducknng_used_duckdb_unstable_api_markdown())  # markdown table

ducknng_read_duckdb_extension_api <- function(header = file.path(
  "duckdb_capi", "duckdb_extension.h"
)) {
  lines <- readLines(header, warn = FALSE)
  current_group <- "stable"
  out <- list()
  n <- 0L

  for (line in lines) {
    version_match <- regexec("^// Version[[:space:]]+([^[:space:]]+)", line)
    version_parts <- regmatches(line, version_match)[[1L]]
    if (length(version_parts)) {
      current_group <- version_parts[[2L]]
      next
    }

    define_match <- regexec("^#define[[:space:]]+(duckdb_[A-Za-z0-9_]+)\\b", line)
    define_parts <- regmatches(line, define_match)[[1L]]
    if (length(define_parts)) {
      n <- n + 1L
      out[[n]] <- data.frame(
        function_name = define_parts[[2L]],
        abi_group = current_group,
        stringsAsFactors = FALSE
      )
    }
  }

  if (!length(out)) {
    return(data.frame(function_name = character(), abi_group = character()))
  }
  do.call(rbind, out)
}

ducknng_strip_c_comments_and_strings <- function(text) {
  text <- paste(text, collapse = "\n")
  text <- gsub("/\\*([^*]|\\*+[^*/])*\\*+/", " ", text, perl = TRUE)
  text <- gsub("//[^\n]*", " ", text, perl = TRUE)
  text <- gsub("\"(\\\\.|[^\"\\\\])*\"", "\"\"", text, perl = TRUE)
  text <- gsub("'(\\\\.|[^'\\\\])*'", "''", text, perl = TRUE)
  text
}

ducknng_extension_source_files <- function(root = ".") {
  list.files(
    file.path(root, "src"),
    pattern = "\\.c$",
    full.names = TRUE
  )
}

ducknng_used_duckdb_unstable_api <- function(root = ".") {
  api <- ducknng_read_duckdb_extension_api(file.path(
    root, "duckdb_capi", "duckdb_extension.h"
  ))
  unstable <- api[startsWith(api$abi_group, "unstable"), , drop = FALSE]
  if (!nrow(unstable)) return(unstable)

  out <- list()
  n <- 0L
  for (file in ducknng_extension_source_files(root)) {
    if (!file.exists(file)) next
    text <- ducknng_strip_c_comments_and_strings(readLines(file, warn = FALSE))
    tokens <- unique(unlist(regmatches(
      text,
      gregexpr("\\bduckdb_[A-Za-z0-9_]+\\b", text, perl = TRUE)
    )))
    used <- intersect(tokens, unstable$function_name)
    if (!length(used)) next

    matched <- merge(
      data.frame(function_name = used, stringsAsFactors = FALSE),
      unstable,
      by = "function_name",
      all.x = TRUE,
      sort = FALSE
    )
    root_norm <- normalizePath(root, mustWork = FALSE)
    matched$file <- sub(
      paste0("^", gsub("([\\.^$|?*+(){}])", "\\\\\\1", root_norm), "/?"),
      "",
      normalizePath(file, mustWork = FALSE)
    )
    n <- n + 1L
    out[[n]] <- matched
  }

  if (!length(out)) {
    return(data.frame(
      function_name = character(),
      abi_group = character(),
      file = character()
    ))
  }

  used <- unique(do.call(rbind, out))
  used[order(used$abi_group, used$function_name, used$file), , drop = FALSE]
}

ducknng_used_duckdb_unstable_api_summary <- function(root = ".") {
  used <- ducknng_used_duckdb_unstable_api(root)
  if (!nrow(used)) {
    return(data.frame(
      abi_group = character(),
      functions = character(),
      count = integer()
    ))
  }

  groups <- split(used$function_name, used$abi_group)
  data.frame(
    abi_group = names(groups),
    functions = vapply(groups, function(x) {
      paste(sprintf("`%s`", sort(unique(x))), collapse = ", ")
    }, character(1L)),
    count = vapply(groups, function(x) length(unique(x)), integer(1L)),
    row.names = NULL,
    stringsAsFactors = FALSE
  )
}

ducknng_used_duckdb_unstable_api_markdown <- function(root = ".") {
  summary <- ducknng_used_duckdb_unstable_api_summary(root)
  if (!nrow(summary)) {
    return("No unstable DuckDB C extension API entries were detected.\n")
  }

  lines <- c(
    "| ABI group | Functions used | Count |",
    "| --- | --- | ---: |",
    sprintf(
      "| `%s` | %s | %d |",
      summary$abi_group, summary$functions, summary$count
    )
  )
  paste0(paste(lines, collapse = "\n"), "\n")
}
