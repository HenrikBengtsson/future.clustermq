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
