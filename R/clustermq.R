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
                     workers = availableCores(), ...) {
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
