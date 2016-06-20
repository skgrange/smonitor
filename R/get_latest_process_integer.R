#' Function to get latest process integer from an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
get_latest_process_integer <- function(con)
  threadr::db_get(con, "SELECT MAX(process) AS process FROM processes")$process
