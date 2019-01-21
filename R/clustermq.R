#' clustermq futures
#'
#' A clustermq future is an asynchronous multiprocess
#' future that will be evaluated in a background R session.
#'
#' @inheritParams ClusterMQFuture
#' 
#' @param workers The number of processes to be available for concurrent
#' clustermq futures.
#' 
#' @param \ldots Additional arguments passed to `ClusterMQFuture()`.
#'
#' @return An object of class [ClusterMQFuture].
#'
#' @details
#' clustermq futures rely on the \pkg{clustermq} package, which is supported
#' on all operating systems.
#'
#' @importFrom future availableCores
#' @export
clustermq <- function(expr, envir = parent.frame(), substitute = TRUE,
                     globals = TRUE, label = NULL,
                     workers = getOption("future.clustermq.workers", availableCores()), ...) {
  if (substitute) expr <- substitute(expr)

  if (is.null(workers)) workers <- availableCores()

  future <- ClusterMQFuture(expr = expr, envir = envir, substitute = FALSE,
                            globals = globals,
                            label = label,
                            workers = workers,
                            ...)

  if (!future$lazy) future <- run(future)

  future
}
class(clustermq) <- c("clustermq", "multiprocess", "future", "function")



#' @importFrom future nbrOfWorkers
#' @export
nbrOfWorkers.clustermq <- function(evaluator) {
  expr <- formals(evaluator)$workers
  workers <- eval(expr, enclos = baseenv())

  if (is.function(workers)) workers <- workers()

  if (inherits(workers, "QSys")) {
    workers <- workers$workers
  }

  stop_if_not(length(workers) == 1L, is.numeric(workers),
              is.finite(workers), workers >= 1L)
  
  workers
}
