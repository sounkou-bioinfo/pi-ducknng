#!/usr/bin/env Rscript

# pkgdown builds the articles and reference; litedown renders the evaluated
# README as the landing page.
pkgdown::build_site(new_process = FALSE, install = FALSE, preview = FALSE)

destination <- "docs"
readme <- readLines("README.md", warn = FALSE, encoding = "UTF-8")
readme <- readme[!grepl("^<!-- README.md is generated", readme)]
readme <- sub("^# pi-ducknng$", "", readme)
metadata <- c(
  "---",
  "title: pi-ducknng",
  "output:",
  "  html:",
  "    options:",
  "      toc: true",
  "    meta:",
  paste0('      css: ["@default@1.14.69", "@article@1.14.69", "',
    normalizePath("tools/landing.css", winslash = "/"), '"]'),
  paste0('      include_before: "',
    normalizePath("tools/landing-header.html", winslash = "/"), '"'),
  "---"
)
litedown::mark(text = c(metadata, readme), output = file.path(destination, "index.html"))
