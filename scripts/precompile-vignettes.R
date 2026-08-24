#!/usr/bin/env Rscript

root <- normalizePath(".", mustWork = TRUE)
if (!file.exists(file.path(root, "DESCRIPTION"))) {
  stop("run this script from the pi-ducknng repository root", call. = FALSE)
}

specs <- list(
  list(
    source = "agent-product-path.Rmd.orig",
    output = "agent-product-path.Rmd",
    receipt = "AGENT_VERIFIED_MTCARS_PERSISTENCE"
  ),
  list(
    source = "agent-active-binding.Rmd.orig",
    output = "agent-active-binding.Rmd",
    receipt = "AGENT_VERIFIED_ACTIVE_BINDING"
  )
)

validate_output <- function(path, receipt) {
  if (!file.exists(path)) {
    stop("missing precompiled vignette: ", path, call. = FALSE)
  }
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  receipt_line <- startsWith(lines, "> ") & grepl(receipt, lines, fixed = TRUE)
  if (!any(receipt_line)) {
    stop("missing live-agent receipt ", receipt, " in ", path, call. = FALSE)
  }
  unavailable <- grepl(
    "\\[pi unavailable\\]|pi not on PATH|not run in this environment",
    lines,
    ignore.case = TRUE
  )
  if (any(unavailable)) {
    stop("live Pi execution was unavailable while producing ", path, call. = FALSE)
  }
  if (any(grepl("^```\\{pi(,|\\})", lines))) {
    stop("precompiled vignette still contains a live pi chunk: ", path,
         call. = FALSE)
  }
  invisible(TRUE)
}

output_dir <- file.path(root, "vignettes")
args <- commandArgs(trailingOnly = TRUE)
check_only <- identical(args, "--check")

if (check_only) {
  for (spec in specs) {
    validate_output(file.path(output_dir, spec$output), spec$receipt)
  }
  cat("precomputed agent vignette receipts: OK\n")
  quit(save = "no", status = 0L)
}

if (length(args) != 0L) {
  stop("usage: precompile-vignettes.R [--check]", call. = FALSE)
}
if (!nzchar(Sys.which("pi"))) {
  stop("pi is required to precompile live-agent vignettes", call. = FALSE)
}
required <- c("knitr", "piknit")
missing <- required[!vapply(required, requireNamespace, logical(1L), quietly = TRUE)]
if (length(missing) > 0L) {
  stop("missing vignette dependencies: ", paste(missing, collapse = ", "),
       call. = FALSE)
}

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
old_options <- options(
  piknit.model = "gpt-5.4",
  piknit.provider = "openai-codex"
)
on.exit(options(old_options), add = TRUE)
piknit::register_engines()
knitr::opts_knit$set(root.dir = root)

staged <- character(length(specs))
tryCatch(
  {
    for (index in seq_along(specs)) {
      spec <- specs[[index]]
      source <- file.path(output_dir, spec$source)
      staged[[index]] <- tempfile(
        pattern = paste0(".", tools::file_path_sans_ext(spec$output), "-"),
        tmpdir = output_dir,
        fileext = ".Rmd"
      )
      knitr::knit(
        input = source,
        output = staged[[index]],
        quiet = FALSE,
        envir = new.env(parent = globalenv())
      )
      lines <- readLines(staged[[index]], warn = FALSE, encoding = "UTF-8")
      writeLines(sub("[[:blank:]]+$", "", lines), staged[[index]], useBytes = TRUE)
      validate_output(staged[[index]], spec$receipt)
    }
    for (index in seq_along(specs)) {
      destination <- file.path(output_dir, specs[[index]]$output)
      if (!file.rename(staged[[index]], destination)) {
        stop("failed to install precomputed vignette: ", destination,
             call. = FALSE)
      }
    }
  },
  finally = unlink(staged[nzchar(staged)])
)

cat("precomputed two vignettes with openai-codex/gpt-5.4 agents\n")
