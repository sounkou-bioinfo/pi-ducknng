wait_for_daemon <- function(profile, timeout = 10) {
  deadline <- Sys.time() + timeout
  repeat {
    if (mirai::status(.compute = profile)$connections == 1L) {
      return(invisible(NULL))
    }
    if (Sys.time() >= deadline) {
      stop("timed out waiting for the mirai daemon")
    }
    Sys.sleep(0.05)
  }
}

send_reply <- function(socket, value) {
  status <- nanonext::send(
    socket,
    charToRaw(value),
    mode = "raw",
    block = TRUE
  )
  if (!identical(status, 0L)) {
    stop("failed to send the NNG reply")
  }
}

main <- function(locator) {
  profile <- paste0("piducknng-endpoint-", Sys.getpid())
  mirai::daemons(1L, .compute = profile)
  on.exit(mirai::daemons(0L, .compute = profile), add = TRUE)
  wait_for_daemon(profile)

  socket <- nanonext::socket("rep", listen = "tcp://127.0.0.1:0")
  on.exit(close(socket), add = TRUE)
  listener <- attr(socket, "listener")[[1L]]
  writeLines(attr(listener, "url"), locator)

  repeat {
    request <- nanonext::recv(socket, mode = "string", block = 30000L)
    if (nanonext::is_error_value(request)) {
      stop("timed out waiting for a ducknng request")
    }

    if (identical(request, "set:x=41")) {
      result <- mirai::everywhere(x <<- 41L, .compute = profile)
      mirai::collect_mirai(result)
      send_reply(socket, "set:x=41")
    } else if (identical(request, "eval:x+1")) {
      value <- mirai::mirai(x + 1L, .compute = profile)[]
      if (!identical(value, 42L)) {
        stop("persistent R evaluation did not return 42")
      }
      send_reply(socket, "42")
    } else if (identical(request, "stop")) {
      send_reply(socket, "stopped")
      break
    } else {
      send_reply(socket, "error:unknown-request")
    }
  }
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) {
  stop("usage: pi-r-endpoint.R LOCATOR_FILE")
}
main(args[[1L]])
