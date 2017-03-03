#' Function to calculate \code{date_start}, \code{date_end}, and 
#' \code{observation_count} for processes and then update the \code{`processes`}
#' table for an \strong{smonitor} database.
#' 
#' @param con Database connection. 
#' 
#' @param tz Time-zone to parse the dates to.
#' 
#' @param progress Type of progress bar to display for the update statements. 
#' 
#' @seealso \code{\link{calculate_process_spans}}, 
#' \code{\link{update_site_spans}}
#' 
#' @author Stuart K. Grange
#' 
#' @export
update_process_spans <- function(con, tz = "UTC", progress = "none") {
  
  # Let database calculate process spans
  # message("Database is aggregating...")
  df <- calculate_process_spans(con, tz = tz) %>% 
    mutate(date_start = stringr::str_replace_na(date_start), 
           date_end = stringr::str_replace_na(date_end))
  
  # Build update statements
  sql <- stringr::str_c(
    "UPDATE processes
     SET date_start='", df$date_start, 
    "',date_end='", df$date_end, 
    "',observation_count=", df$observation_count,
    " WHERE process=", df$process, "")
  
  # No quotes for NULL
  sql <- stringr::str_replace_all(sql, "'NA'", "NULL")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Use statements
  databaser::db_execute(con, sql, progress = progress)
  
  # No return
  
}
