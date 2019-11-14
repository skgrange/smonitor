#' Function to delete/purge all observations for a process from the 
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
#' @param by_process Should the function use multiple SQL statements to delete 
#' processes? This is useuful if there are many to delete and a progress bar
#' is desired. 
#' 
#' @param progress If \code{by_process}, should a progress bar be displayed? 
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
delete_process <- function(con, process, by_process = FALSE, progress = FALSE) {
  
  if (by_process) {
    
    # Build many statements
    sql <- stringr::str_c(
      "DELETE FROM observations
       WHERE process=", process, ""
    ) %>% 
      stringr::str_squish()
    
    # Use statements and show progress
    databaser::db_execute(con, sql, progress = if_else(progress, "time", "none"))
    
  } else {
    
    # Collapse all processes and send one statement
    process <- stringr::str_c(process, collapse = ",")
    
    # Build statement
    sql <- stringr::str_c(
      "DELETE FROM observations
       WHERE process IN (", process, ")"
    ) %>% 
      stringr::str_squish()
    
    # Use statement to kill observations
    databaser::db_execute(con, sql)
    
  }
  
  return(invisible(con))
  
}
