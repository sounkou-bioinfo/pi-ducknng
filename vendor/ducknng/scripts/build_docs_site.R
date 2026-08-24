#!/usr/bin/env Rscript

# Builds the curated ducknng documentation site into _site/ with litedown.
# The site publishes a deliberately small subset of docs/: the contracts a
# user needs to talk to a ducknng service. Everything else under docs/ stays
# in-repo for contributors rather than becoming a public page.

if (!requireNamespace("litedown", quietly = TRUE)) {
  stop("Package 'litedown' is required to build the documentation site", call. = FALSE)
}

pages <- c(
  index = "docs/landing.md",
  protocol = "docs/protocol.md",
  reference = "docs/function_reference.md",
  types = "docs/types.md",
  transports = "docs/transports.md",
  http = "docs/http.md",
  browser = "docs/browser_support.md",
  security = "docs/security.md"
)
titles <- c(
  index = "ducknng",
  protocol = "Protocol specification · ducknng",
  reference = "SQL function reference · ducknng",
  types = "Supported types · ducknng",
  transports = "Transports · ducknng",
  http = "HTTP carrier · ducknng",
  browser = "Browser support · ducknng",
  security = "Security model · ducknng"
)

missing_sources <- pages[!file.exists(pages)]
if (length(missing_sources) > 0L) {
  stop(
    "Curated site sources are missing: ",
    paste(sprintf("%s (%s)", names(missing_sources), missing_sources), collapse = ", "),
    call. = FALSE
  )
}

site_dir <- "_site"
unlink(site_dir, recursive = TRUE, force = TRUE)
dir.create(site_dir, recursive = TRUE, showWarnings = FALSE)
invisible(file.create(file.path(site_dir, ".nojekyll")))

css <- normalizePath("tools/site.css", winslash = "/", mustWork = TRUE)
header <- normalizePath("tools/site-header.html", winslash = "/", mustWork = TRUE)

build_metadata <- function(include_before, toc = TRUE) {
  c(
    "---",
    "output:",
    "  html:",
    "    options:",
    paste0("      toc: ", if (isTRUE(toc)) "true" else "false"),
    "    meta:",
    paste0(
      "      css: [\"@default@1.14.69\", \"@article@1.14.69\", ",
      "\"@site@1.14.69\", \"", css, "\"]"
    ),
    # Marks the contents entry for the section currently in view. Targets #TOC
    # directly, so it works with the grid sidebar in tools/site.css and does not
    # need litedown's sidenote script (which also relocates footnotes).
    "      js: [\"@toc-highlight@1.14.69\"]",
    paste0("      include_before: \"", include_before, "\""),
    "---"
  )
}

# docs/landing.md is a purpose-built landing page rather than the README: it
# carries a hero, an SVG topology diagram, numbered steps, and card grids that
# would render poorly in a plain-text README. README.md stays the full
# GitHub-facing document. The landing page is short and already structured, so
# it renders without a table of contents; every other page keeps the sidebar.

for (name in names(pages)) {
  source <- pages[[name]]
  destination <- file.path(site_dir, paste0(name, ".html"))
  markdown <- readLines(source, warn = FALSE, encoding = "UTF-8")
  message("rendering ", source, " -> ", destination)
  litedown::mark(
    text = c(build_metadata(header, toc = name != "index"), markdown),
    output = destination,
    meta = list("plain-title" = titles[[name]])
  )
}

required <- file.path(site_dir, paste0(names(pages), ".html"))
missing <- required[!file.exists(required) | file.info(required)$size == 0]
if (length(missing) > 0L) {
  stop("Site build did not produce: ", paste(missing, collapse = ", "), call. = FALSE)
}

message("built ", length(required), " pages into ", site_dir, "/")
