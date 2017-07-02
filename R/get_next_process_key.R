#' Function to get next \code{process} key for a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Interger with length of 1. 
#' 
#' @export
get_next_process_key <- function(con) {
  
  x <- databaser::db_get(con, "SELECT max(process) FROM processes")[, 1]
  x <- ifelse(is.na(x), 1, x + 1)
  x
  
}
