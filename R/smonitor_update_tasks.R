#' Function to update things in an \strong{smonitor} database.
#' 
#' \code{update_smonitor_things} will update variables in the \code{`processes`}, 
#' \code{`sites`}, and \code{`row_counts`} tables and is intended to be run as
#' as scheduled task. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param tz Time-zone for dates. 
#' 
#' @export
smonitor_update_tasks <- function(con, tz = "UTC") {
  
  # Update variables in process
  update_process_spans(con, tz = tz, progress = "none")
  
  # Update variables in sites
  update_site_spans(con, tz = tz,  progress = "none")
  
  # Update row counts
  update_row_counts(con, estimate = FALSE, read = FALSE)
  
  # No return
  
}
