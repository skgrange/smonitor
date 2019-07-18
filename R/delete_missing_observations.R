#' Function to delete missing (\code{NA}/\code{NULL}) value from the 
#' \code{`observations`} table in an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange.
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
delete_missing_observations <- function(con) {
  databaser::db_execute(con, "DELETE FROM observations WHERE value IS NULL")
  return(invisible(con))
}
