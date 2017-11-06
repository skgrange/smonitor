#' Function to get next \code{process} key for a \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database. 
#' 
#' @param minimum If \code{positive}, what is the minimum value of the process 
#' key? Useful for when process keys want to be grouped together. 
#' 
#' @param positive Should the next process key be greater than the current 
#' process in the database? If \code{FALSE}, negative sequences are usually 
#' to be generated. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Integer with length of 1. 
#' 
#' @export
get_next_process_key <- function(con, minimum = NA, positive = TRUE) {
  
  # Check
  stopifnot(length(minimum) == 1)
  
  if (positive) {
    
    # Get max process
    x <- databaser::db_get(con, "SELECT max(process) FROM processes")[, 1]
    x <- ifelse(is.na(x), 1, x + 1)
    
    # 
    if (!is.na(minimum)) x <- ifelse(x < minimum, minimum, x)
    
  } else {
    
    # Get min process
    x <- databaser::db_get(con, "SELECT min(process) FROM processes")[, 1]
    x <- ifelse(is.na(x) | x %in% c(0, 1), -1, x - 1)
    
  }
  
  return(x)
  
}
