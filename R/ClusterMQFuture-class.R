#' A clustermq future is a future whose value will be resolved via clustermq
#'
#' @param expr The \R expression to be evaluated.
#'
#' @param envir The environment in which global environment
#' should be located.
#'
#' @param substitute Controls whether `expr` should be `substitute()`:d
#' or not.
#'
#' @param globals (optional) a logical, a character vector, a named list, or
#' a [globals::Globals] object.  If `TRUE`, globals are identified by code
#' inspection based on `expr` and `tweak` searching from environment
#' `envir`.  If `FALSE`, no globals are used.  If a character vector, then
#' globals are identified by lookup based their names `globals` searching
#' from environment `envir`.  If a named list or a Globals object, the
#' globals are used as is.
#'
#' @param label (optional) Label of the future.
#'
#' @param workers (optional) The maximum number of workers the clustermq
#' backend may use at any time.
#'
#' @param \ldots Additional arguments passed to [future::MultiprocessFuture()].
#'
#' @return A ClusterMQFuture object
#'
#' @aliases run.ClusterMQFuture
#' @export
#' @importFrom future MultiprocessFuture getGlobalsAndPackages
#' @keywords internal
ClusterMQFuture <- function(expr = NULL, envir = parent.frame(),
                            substitute = TRUE,
                            globals = TRUE, packages = NULL,
                            label = NULL,
                            workers = NULL,
                            ...) {
  if (substitute) expr <- substitute(expr)

  if (!is.null(label)) label <- as.character(label)
  
  if (is.function(workers)) workers <- workers()
  if (!is.null(workers)) {
    stop_if_not(length(workers) >= 1)
    if (is.numeric(workers)) {
      stop_if_not(!anyNA(workers), all(workers >= 1))
    } else {
      stop("Argument 'workers' should be numeric: ", mode(workers))
    }
  }

  ## Record globals
  gp <- getGlobalsAndPackages(expr, envir = envir, globals = globals)

  ## Create ClusterMQFuture object
  future <- MultiprocessFuture(expr = gp$expr, envir = envir,
                               substitute = FALSE, workers = workers,
                               label = label, version = "1.8", ...)
  future$.callResult <- TRUE

  future$globals <- gp$globals
  future$packages <- unique(c(packages, gp$packages))
  future$state <- "created"

  future <- structure(future, class = c("ClusterMQFuture", class(future)))

  future
}


#' Prints a clustermq future
#'
#' @param x An ClusterMQFuture object
#' 
#' @param \ldots Not used.
#'
#' @export
#' @keywords internal
print.ClusterMQFuture <- function(x, ...) {
  NextMethod()

  ## Ask for status once
  worker <- x$worker
  if (inherits(worker, "QSys")) {
    cat(paste(c(capture_output(print(worker)), ""), collapse = "\n"))
  } else {
    cat("No clustermq QSys worker set")
  }
  invisible(x)
}

#' @export
getExpression.ClusterMQFuture <- function(future, mc.cores = 1L, ...) {
  NextMethod(mc.cores = mc.cores)
}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Future API
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#' @importFrom future resolved
#' @keywords internal
#' @export
resolved.ClusterMQFuture <- function(x, ...) {
  if (inherits(x$result, "FutureResult")) return(TRUE)
  process <- x$process
  if (!inherits(process, "r_process")) return(FALSE)
  !process$is_alive()
}

#' @importFrom future result UnexpectedFutureResultError
#' @keywords internal
#' @export
result.ClusterMQFuture <- function(future, ...) {
  result <- future$result
  if (!is.null(result)) {
    if (inherits(result, "FutureError")) stop(result)
    return(result)
  }
  
  if (future$state == "created") {
    future <- run(future)
  }

  result <- await(future, cleanup = FALSE)

  if (!inherits(result, "FutureResult")) {
    ex <- UnexpectedFutureResultError(future)
    future$result <- ex
    stop(ex)
  }

  future$result <- result
  future$state <- "finished"
  
  result
}


#' @importFrom future run getExpression FutureError
#' @keywords internal
#' @S3method run ClusterMQFuture
#' @export
run.ClusterMQFuture <- local({
  FutureRegistry <- import_future("FutureRegistry")
  mdebug <- import_future("mdebug")
  assertOwner <- import_future("assertOwner")

  function(future, ...) {
    if (future$state != "created") {
      label <- future$label
      if (is.null(label)) label <- "<none>"
      msg <- sprintf("A future ('%s') can only be launched once.", label)
      stop(FutureError(msg, future = future))
    }
  
    ## Assert that the process that created the future is
    ## also the one that evaluates/resolves/queries it.
    assertOwner(future)
  
    ## Temporarily disable clustermq output?
    ## (i.e. messages and progress bars)
    debug <- getOption("future.debug", FALSE)
  
    ## Get future expression
    stdout <- if (isTRUE(future$stdout)) TRUE else NA
    expr <- getExpression(future, stdout = stdout)
  
    ## Get globals
    globals <- future$globals

    worker <- future$worker
    stop_if_not(inherits(worker, "QSys"))

    ## 2. Allocate future now worker
#    FutureRegistry("workers-clustermq", action = "add", future = future, earlySignal = FALSE)
  
    ## Launch
    ref <- sprintf("%s-%s", class(future)[1], future$owner)
    stopifnot(is.character(ref), length(ref) == 1L, !is.na(ref), nzchar(ref))
    success <- worker$send_call(expr, env=globals, ref = ref)
    if (debug) mdebug("Launch success: %s", success)
  
    ## 3. Running
    future$state <- "running"
  
    invisible(future)
  } ## run()
})


await <- function(...) UseMethod("await")

#' Awaits the result of a clustermq future
#'
#' @param future The future.
#' 
#' @param timeout Total time (in seconds) waiting before generating an error.
#' 
#' @param delta The number of seconds to wait between each poll.
#' 
#' @param alpha A factor to scale up the waiting time in each iteration such
#' that the waiting time in the k:th iteration is `alpha ^ k * delta`.
#' 
#' @param \ldots Not used.
#'
#' @return The FutureResult of the evaluated expression.
#' If an error occurs, an informative Exception is thrown.
#'
#' @details
#' Note that `await()` should only be called once, because
#' after being called the actual asynchronous future may be removed
#' and will no longer available in subsequent calls.  If called
#' again, an error may be thrown.
#'
#' @export
#' @importFrom utils tail
#' @importFrom future FutureError FutureWarning
#' @keywords internal
await.ClusterMQFuture <- local({
  FutureRegistry <- import_future("FutureRegistry")
  mdebug <- import_future("mdebug")

  function(future, timeout = getOption("future.wait.timeout", 30*24*60*60),
                   delta = getOption("future.wait.interval", 1.0),
                   alpha = getOption("future.wait.alpha", 1.01),
                   ...) {
    stop_if_not(is.finite(timeout), timeout >= 0)
    stop_if_not(is.finite(alpha), alpha > 0)
    
    debug <- getOption("future.debug", FALSE)
  
    expr <- future$expr
    worker <- future$worker
    stop_if_not(inherits(worker, "QSys"))
  
    if (debug) mdebug("Wait for clustermq worker ...")
  
    ## Sleep function - increases geometrically as a function of iterations
    sleep_fcn <- function(i) delta * alpha ^ (i - 1)
    sleep_fcn <- function(i) Inf

    ## Poll process
    t_timeout <- Sys.time() + timeout
    ii <- 1L
    msg <- list()
    while (is.null(msg$result)) {
      ## Timed out?
      if (Sys.time() > t_timeout) break
      timeout_ii <- sleep_fcn(ii)
      if (debug && ii %% 100 == 0)
        mdebug("- iteration %d: clustermq::wait(timeout = %g)", ii, timeout_ii)
      msg <- worker$receive_data(timeout = timeout_ii)
      if (is.null(msg$result)) {
        stop(FutureError("Should never(?) happen"))
      }
      ii <- ii + 1L
    }
  
    if (debug) {
      mdebug("- clustermq worker: finished")
      if (debug) mdebug("Wait for clustermq worker ... done")
    }
  
    if (debug) {
      mdebug("Results:")
      mstr(result)
    }
    
#    FutureRegistry("workers-clustermq", action = "remove", future = future)
    
    result
  } # await()
})