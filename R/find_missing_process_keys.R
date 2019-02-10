#' Function to get missing process keys for a \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Integer vector. 
#' 
#' @export
find_missing_process_keys <- function(con) {
  
  # Get all process keys
  processes <- databaser::db_get(
    con,
    "SELECT process
    FROM processes
    ORDER BY process"
  ) %>% 
    pull()
  
  # Generate a linear seqeunce
  sequence <- seq(from = 1, to = max(processes), by = 1)
  
  # Get the values from the sequence which have not been used
  processes_outstanding <- sequence[which(!sequence %in% processes)]
  processes_outstanding <- as.integer(processes_outstanding)
  
  return(processes_outstanding)
  
}
