# Shared setup for the precomputed guides and the README.
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", error = FALSE)
source("tools/ducknng-rpc.R")
package_root <- normalizePath(".")
guide_state <- tempfile("pi-ducknng-guide-")
dir.create(guide_state, mode = "0700")
Sys.setenv(STATE = guide_state)

# The agents' project directory. Its .pi/ducknng/workspace.duckdb is the SQL
# workspace they share across sessions.
guide_project <- file.path(guide_state, "project")
dir.create(guide_project, mode = "0700")

quoted <- function(lines) paste0("> ", lines, collapse = "\n")

# Gives a live Pi agent one task in the guide's project and prints the task and
# its answer. The agent loads only this package's extension. A failed run stops
# the document rather than recording an error as an answer.
agent_task <- function(task, agent_id = NULL, project = guide_project) {
  if (!is.null(agent_id)) {
    previous <- Sys.getenv("PI_DUCKNNG_AGENT_ID", unset = NA)
    Sys.setenv(PI_DUCKNNG_AGENT_ID = agent_id)
    on.exit(
      if (is.na(previous)) Sys.unsetenv("PI_DUCKNNG_AGENT_ID") else Sys.setenv(PI_DUCKNNG_AGENT_ID = previous),
      add = TRUE
    )
  }
  task <- trimws(gsub("\\s+", " ", task))
  reply <- piknit::pi_run(
    task,
    provider = "openai-codex", model = "gpt-6-astra", thinking = "medium",
    extension = file.path(package_root, "extensions/pi-ducknng/index.ts"),
    no_extensions = TRUE, timeout = 900, dir = project
  )
  status <- attr(reply, "status", exact = TRUE)
  lines <- as.character(reply)
  if (identical(lines, "[pi unavailable]") || (!is.null(status) && status != 0)) {
    stop("the live agent failed:\n", paste(lines, collapse = "\n"), call. = FALSE)
  }
  # Paths print relative to the checkout, as the rest of the guides do.
  lines <- gsub(package_root, ".", lines, fixed = TRUE)
  who <- if (is.null(agent_id)) "The agent" else paste0("Agent `", agent_id, "`")
  cat("**Task**\n\n", quoted(task), "\n\n**", who, " answered**\n\n",
      quoted(lines), "\n", sep = "")
  invisible(lines)
}

# Runs SQL against the project's workspace file with ducknng loaded, as any
# DuckDB client could once the agents have exited, and returns the rows.
workspace_sql <- function(sql, project = guide_project) {
  database <- file.path(project, ".pi/ducknng/workspace.duckdb")
  script <- tempfile(fileext = ".sql")
  on.exit(unlink(script), add = TRUE)
  writeLines(c("COPY (", sub(";\\s*$", "", sql), ") TO '/dev/stdout' (FORMAT csv, HEADER);"), script)
  output <- system2("node", c("tools/ducknng-sql.ts", "--database", shQuote(database), shQuote(script)),
                    stdout = TRUE, stderr = TRUE)
  if (!is.null(attr(output, "status"))) stop(paste(output, collapse = "\n"), call. = FALSE)
  utils::read.csv(text = output, stringsAsFactors = FALSE)
}

# Prints selected reply fields as JSON.
show <- function(value, fields = names(value)) {
  cat(jsonlite::toJSON(value[fields], auto_unbox = TRUE, null = "null", pretty = TRUE), "\n")
}

# Returns the coordination error text a call fails with.
failure <- function(expr) {
  tryCatch(
    {
      force(expr)
      "no error"
    },
    error = function(error) {
      message <- conditionMessage(error)
      codes <- "invalid_argument|registration_expired|unauthorized|idempotency_conflict|mailbox_full|receipt_invalid|lease_invalid|resource_conflict"
      sub(paste0("^.*?((", codes, "): )"), "\\1", message, perl = TRUE)
    }
  )
}

guide_url <- function(name = "coordination") {
  readLines(file.path(guide_state, paste0(name, ".url")))
}

register <- function(agent, instance = paste0(agent, "-1"), project = "guide",
                     url = guide_url(), ...) {
  rpc_call(url, "register", list(
    project_id = project, agent_id = agent,
    instance_id = instance, operation_key = "start", ...
  ))
}
