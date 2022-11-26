#' Function to import data from an \strong{smonitor} \code{`smonitor_details`} 
#' table. 
#' 
#' @param con Database connection for an \strong{smonitor} database. 
#' 
#' @param tz What time zone should the \code{date} variable be represented in? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble.
#' 
#' @export
import_smonitor_details <- function(con, tz = "UTC") {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "smonitor_details"))
  
  # Get all data and parse date
  databaser::db_get(
    con, 
    "SELECT * 
    FROM smonitor_details
    ORDER BY date"
  ) %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz))
  
}
