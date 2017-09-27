#' Function to delete all observations for a process from the 
#' \code{`observations`} table in a \strong{smonitor} database. 
#' 
#' \code{delete_process} is usually used when a process needs to be purged and 
#' will delete all summaries associated with a process too. Use with care. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param process A vector of processes. 
#' 
#' @export
delete_process <- function(con, process) {
  
  # Collapse
  process <- stringr::str_c(process, collapse = ",")
  
  # Build statement
  sql <- stringr::str_c(
    "DELETE FROM observations
     WHERE process IN (", process, ")"
  )
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Use statement to kill observations
  databaser::db_execute(con, sql)
  
  # No return
  
}
