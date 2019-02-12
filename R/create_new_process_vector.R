#' Function to generate a vector of \strong{smonitor} process keys. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
#' 
#' @param n Number of process keys to generate. 
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
#' @return Integer vector with the length of \code{n}. 
#' 
#' @export
create_new_process_vector <- function(con, n, minimum = NA, positive = TRUE) {
  
  x <- get_next_process_key(con, minimum = minimum, positive = positive)
  
  if (positive) {
    x <- seq(x, length.out = n)
  } else {
    x <- seq(from = x, to = x + (-n+ 1))
  }
  
  # Ensure integer
  x <- as.integer(x)
  
  return(x)
  
}
