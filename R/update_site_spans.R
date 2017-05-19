#' Function to update \code{date_start} and \code{date_end}, for sites and then
#' update the \code{`sites`} table for an \strong{smonitor} database.
#' 
#' Use \code{update_site_span} after \code{\link{update_process_spans}} because 
#' it queries the \code{`processes`} table rather than \code{`observations`}. 
#' 
#' @param con Database connection. 
#' @param tz Time-zone to parse the dates to.
#' 
#' @seealso \code{\link{update_process_spans}}
#'
#' @export
update_site_spans <- function(con, tz = "UTC") {
  
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
              date_end = max(date_end, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(date_start = stringr::str_replace_na(date_start),
           date_end = stringr::str_replace_na(date_end))
  
  # Build update statements
  sql <- stringr::str_c(
    "UPDATE sites
     SET date_start='", df$date_start, 
    "',date_end='", df$date_end, 
    "' WHERE site='", df$site, "'"
  )
  
  # Make nulls
  sql <- stringr::str_replace_all(sql, "'NA'", "NULL")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Use statements
  databaser::db_execute(con, sql)
  
  # No return
  
}
