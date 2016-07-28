#' Function to calculate \code{date_start}, \code{date_end}, and 
#' \code{observation_count} for processes and then update the \code{`processes`}
#' table for an \strong{smonitor} database.
#' 
#' @param con Database connection. 
#' @param tz Time-zone to parse the dates to.
#' @param progress Type of progress bar to display for the update statements. 
#' Default is \code{"text"}. 
#' 
#' @seealso \link{\code{calculate_process_spans}}
#' 
#' @author Stuart K. Grange
#' 
#' @export
update_process_span_variables <- function(con, tz = "UTC", progress = "text") {
  
  # Let database calculate process spans
  message("Database is aggregating...")
  df <- calculate_process_spans(con, tz = tz)
  
  # Build update statements
  sql <- stringr::str_c(
    "UPDATE processes
     SET date_start='", df$date_start, 
    "', date_end='", df$date_end, 
    "', observation_count=", df$observation_count,
    " WHERE process=", df$process, "")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Use statements
  message("Updating 'processes' table...")
  databaser::db_send(con, sql, progress = progress)
  
  # No return
  
}
