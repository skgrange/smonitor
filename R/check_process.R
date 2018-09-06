#' Function to test if processes exist in the `processes` table. 
#' 
#' @param con Database connection to a smonitor database. 
#' 
#' @param process A vector of processes to test. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Integer vector, if length is 0, the processes already exist. 
#' 
#' @export
check_process <- function(con, process) {
  
  # Parse input
  process <- sort(unique(process))
  
  # For sql
  process_collapse <- stringr::str_c(process, collapse = ",")
  
  # Get processes in `processes`
  processes_in_processes <- databaser::db_get(
    con, 
    stringr::str_c(
      "SELECT process 
      FROM processes
      WHERE process IN (", process_collapse, ")"
    )
  ) %>% 
    pull()
  
  # Test, return processes which do not exist
  x <- setdiff(process, processes_in_processes)
  
  return(x)
  
}
