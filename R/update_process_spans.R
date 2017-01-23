#' Function to calculate \code{date_start}, \code{date_end}, and 
#' \code{observation_count} for processes and then update the \code{`processes`}
#' table for an \strong{smonitor} database.
#' 
#' @param con Database connection. 
#' 
#' @param tz Time-zone to parse the dates to.
#' 
#' @param progress Type of progress bar to display for the update statements. 
#' Default is \code{"text"}. 
#' 
#' @seealso \code{\link{calculate_process_spans}}, 
#' \code{\link{update_site_spans}}
#' 
#' @author Stuart K. Grange
#' 
#' @export
update_process_spans <- function(con, tz = "UTC", progress = "text") {
  
  # Let database calculate process spans
  # message("Database is aggregating...")
  df <- calculate_process_spans(con, tz = tz)
  
  # Build update statements
  # No nas will be created
  sql <- stringr::str_c(
    "UPDATE processes
     SET date_start='", df$date_start, 
    "', date_end='", df$date_end, 
    "', observation_count=", df$observation_count,
    " WHERE process=", df$process, "")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Use statements
  # message("Updating 'processes' table...")
  databaser::db_execute(con, sql, progress = progress)
  
  # No return
  
}
