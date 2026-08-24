#!/usr/bin/env Rscript

wait_for_connection <- function(profile, timeout = 15) {
  deadline <- Sys.time() + timeout
  repeat {
    if (mirai::status(.compute = profile)$connections == 1L) {
      return(invisible(TRUE))
    }
    if (Sys.time() >= deadline) {
      stop("timed out waiting for the persistent R daemon")
    }
    Sys.sleep(0.05)
  }
}

daemon_process <- function(url) {
  mirai::daemon(
    url,
    dispatcher = FALSE,
    asyncdial = TRUE,
    autoexit = FALSE,
    cleanup = FALSE,
    walltime = 60000L
  )
}

host_a <- function(url, profile, locator, daemon_log) {
  mirai::daemons(url = url, dispatcher = FALSE, .compute = profile)
  resolved_url <- mirai::status(.compute = profile)$daemons

  script_arg <- grep("^--file=", commandArgs(), value = TRUE)
  script <- normalizePath(sub("^--file=", "", script_arg[[1L]]))
  rscript <- file.path(R.home("bin"), "Rscript")
  status <- system2(
    rscript,
    c("--vanilla", shQuote(script), "daemon", shQuote(resolved_url),
      shQuote(profile)),
    stdout = daemon_log,
    stderr = daemon_log,
    wait = FALSE
  )
  stopifnot(identical(status, 0L))
  wait_for_connection(profile)

  setup <- mirai::everywhere(x <<- 41L, .compute = profile)
  mirai::collect_mirai(setup)
  stopifnot(identical(mirai::mirai(x, .compute = profile)[], 41L))
  writeLines(resolved_url, locator)
}

host_b <- function(url, profile) {
  mirai::daemons(url = url, dispatcher = FALSE, .compute = profile)
  on.exit(mirai::daemons(NULL, .compute = profile), add = TRUE)
  wait_for_connection(profile)

  value <- mirai::mirai(x + 1L, .compute = profile)[]
  stopifnot(identical(value, 42L))
}

run_host <- function(mode, url, profile, log, extra = character()) {
  script_arg <- grep("^--file=", commandArgs(), value = TRUE)
  script <- normalizePath(sub("^--file=", "", script_arg[[1L]]))
  rscript <- file.path(R.home("bin"), "Rscript")
  status <- system2(
    rscript,
    c("--vanilla", shQuote(script), mode, shQuote(url), shQuote(profile),
      vapply(extra, shQuote, character(1L))),
    stdout = log,
    stderr = log
  )
  if (!identical(status, 0L)) {
    stop(paste(readLines(log, warn = FALSE), collapse = "\n"), call. = FALSE)
  }
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) > 0L) {
    mode <- args[[1L]]
    url <- args[[2L]]
    profile <- args[[3L]]
    if (identical(mode, "daemon")) {
      daemon_process(url)
    } else if (identical(mode, "host-a")) {
      host_a(url, profile, args[[4L]], args[[5L]])
    } else if (identical(mode, "host-b")) {
      host_b(url, profile)
    } else {
      stop("unknown proof mode: ", mode)
    }
    return(invisible(NULL))
  }

  url <- unname(mirai::local_url(tcp = TRUE))
  profile <- paste0("piducknng-session-owner-", Sys.getpid())
  locator <- tempfile("piducknng-locator-", fileext = ".txt")
  log_daemon <- tempfile("piducknng-daemon-", fileext = ".log")
  log_a <- tempfile("piducknng-host-a-", fileext = ".log")
  log_b <- tempfile("piducknng-host-b-", fileext = ".log")
  on.exit(unlink(c(locator, log_daemon, log_a, log_b)), add = TRUE)

  tryCatch(
    {
      run_host("host-a", url, profile, log_a, c(locator, log_daemon))
      resolved_url <- readLines(locator, warn = FALSE)
      stopifnot(length(resolved_url) == 1L)
      run_host("host-b", resolved_url, profile, log_b)
    },
    error = function(error) {
      daemon_output <- paste(readLines(log_daemon, warn = FALSE), collapse = "\n")
      stop(conditionMessage(error), "\ndaemon output:\n", daemon_output,
           call. = FALSE)
    }
  )
  cat("persistent R daemon reconnected with x + 1 = 42\n")
}

main()
