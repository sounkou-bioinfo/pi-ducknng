# Shared setup for the precomputed coordination guides.
knitr::opts_chunk$set(collapse = TRUE, comment = "#>", error = FALSE)
source("tools/ducknng-rpc.R")
guide_state <- tempfile("pi-ducknng-guide-")
dir.create(guide_state, mode = "0700")
Sys.setenv(STATE = guide_state)

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
