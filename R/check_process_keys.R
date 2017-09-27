#' Function to check if a vector of process keys are allocated in the processes
#' table in an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param process Process vector. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible, an error if the test fails. 
#' 
#' @export
check_process_keys <- function(con, process) {
  
  process_collapsed <- stringr::str_c(process, collapse = ",")
  
  sql <- stringr::str_c(
    "SELECT process 
     FROM processes 
     WHERE process IN (", process_collapsed, ")"
  )
  
  df <- databaser::db_get(con, sql)
  
  if (nrow(df) != 0) stop("Duplicate process keys...", call. = FALSE)
  
  # No return
  
}
