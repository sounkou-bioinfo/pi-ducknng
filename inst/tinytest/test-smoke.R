description <- utils::packageDescription("piducknng")
imports <- strsplit(description$Imports, ",", fixed = TRUE)[[1L]]
imports <- trimws(sub("\\s*\\(.*\\)$", "", imports))

expect_equal(
  sort(imports),
  sort(c("DBI", "duckdb", "jsonlite", "mirai", "nanoarrow", "nanonext"))
)
expect_equal(getNamespaceExports("piducknng"), character())
