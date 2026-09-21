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
  ),
  list(
    source = "coordination-mailbox.Rmd.orig",
    output = "coordination-mailbox.Rmd",
    receipt = "GUIDE_MAILBOX_VERIFIED"
  ),
  list(
    source = "coordination-fanout.Rmd.orig",
    output = "coordination-fanout.Rmd",
    receipt = c("AGENT_WORKER_REPLIED w-mean", "AGENT_WORKER_REPLIED w-median",
                "AGENT_BROADCAST_SENT", "GUIDE_FANOUT_VERIFIED")
  ),
  list(
    source = "coordination-reservations.Rmd.orig",
    output = "coordination-reservations.Rmd",
    receipt = c("AGENT_EDIT_BLOCKED", "GUIDE_RESERVATIONS_VERIFIED")
  ),
  list(
    source = "coordination-durability.Rmd.orig",
    output = "coordination-durability.Rmd",
    receipt = "GUIDE_DURABILITY_VERIFIED"
  ),
  list(
    source = "coordination-mtls.Rmd.orig",
    output = "coordination-mtls.Rmd",
    receipt = "GUIDE_MTLS_VERIFIED"
  ),
  list(
    source = "coordination-harness.Rmd.orig",
    output = "coordination-harness.Rmd",
    receipt = "GUIDE_HARNESS_VERIFIED"
  )
)

validate_output <- function(path, receipt) {
  if (!file.exists(path)) {
    stop("missing precompiled vignette: ", path, call. = FALSE)
  }
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  for (expected in receipt) {
    receipt_line <- grepl(paste0("^(> |#> )?", expected), lines) |
      (startsWith(lines, "> ") & grepl(expected, lines, fixed = TRUE))
    if (!any(receipt_line)) {
      stop("missing receipt ", expected, " in ", path, call. = FALSE)
    }
  }
  if (any(grepl(root, lines, fixed = TRUE))) {
    stop("precompiled vignette contains the checkout path: ", path, call. = FALSE)
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
# --only name[,name] rebuilds a subset of the guides.
if (length(args) == 2L && identical(args[[1L]], "--only")) {
  wanted <- strsplit(args[[2L]], ",", fixed = TRUE)[[1L]]
  names <- vapply(specs, function(spec) tools::file_path_sans_ext(spec$output), "")
  unknown <- setdiff(wanted, names)
  if (length(unknown) > 0L) stop("unknown vignette: ", unknown[[1L]], call. = FALSE)
  specs <- specs[names %in% wanted]
  args <- character()
}

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
if (!nzchar(Sys.which(Sys.getenv("PIKNIT_PI", unset = "pi")))) {
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

cat("precomputed", length(specs), "vignettes\n")
