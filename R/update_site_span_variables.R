#' Function to update \code{date_start} and \code{date_end}, for sites and then
#' update the \code{`sites`} table for an \strong{smonitor} database.
#' 
#' Use \code{update_site_span_variables} after 
#' \code{\link{update_process_span_variables}} because it queries the 
#' \code{`processes`} table rather than \code{`observations`}. 
#' 
#' @param con Database connection. 
#' @param tz Time-zone to parse the dates to.
#' @param progress Type of progress bar to display for the update statements. 
#' Default is \code{"none"}. 
#'
#' @seealso \code{\link{update_process_span_variables}}
#'
#' @export
update_site_span_variables <- function(con, tz = "UTC", progress = "none") {
  
  # Get data and transform
  df <- databaser::db_get(con, "SELECT site,
                                date_start, 
                                date_end
                                FROM processes") %>% 
    mutate(date_start = lubridate::ymd_hms(date_start, tz = tz, truncated = 3),
           date_end = lubridate::ymd_hms(date_end, tz = tz, truncated = 3))
  
  # Summarise
  df <- df %>% 
    group_by(site) %>% 
    summarise(date_start = min(date_start, na.rm = TRUE),
              date_end = min(date_end, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(date_start = stringr::str_replace_na(date_start),
           date_end = stringr::str_replace_na(date_end))
  
  # Build update statements
  sql <- stringr::str_c(
    "UPDATE sites
     SET date_start='", df$date_start, 
    "',date_end='", df$date_end, 
    "' WHERE site='", df$site, "'")
  
  # Make nulls
  sql <- stringr::str_replace_all(sql, "'NA'", "NULL")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Use statements
  # message("Updating 'sites' table...")
  databaser::db_send(con, sql, progress = progress)
  
  # No return
  
}
