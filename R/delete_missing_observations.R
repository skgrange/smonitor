#' Function to delete missing (\code{NA}/\code{NULL}) value from the 
#' \code{`observations`} table in an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange.
#' 
#' @return Invisible. 
#' 
#' @export
delete_missing_observations <- function(con) {
  
  sql <- "DELETE FROM observations WHERE value IS NULL"
  databaser::db_execute(con, sql)
  # No return
  
}
