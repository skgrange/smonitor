#' Function to import the \code{`trend_tests`} table from an \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
#' 
#' @param tz What time zone should the \code{date_*} variables be represented in? 
#' 
#' @param date_insert Should the return include the \code{date_insert} variable?
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble.
#' 
#' @export
import_trend_tests <- function(con, tz = "UTC", date_insert = FALSE) {
  
  # Check for table
  if (!databaser::db_table_exists(con, "trend_tests")) {
    stop("The `trend_tests` table does not exist...", call. = FALSE)
  }
  
  # Get table
  df <- databaser::db_get(
    con, 
    "SELECT * 
    FROM trend_tests
    ORDER BY site, 
    variable,
    summary"
  ) %>% 
    mutate(date_insert = threadr::parse_unix_time(date_insert, tz = tz),
           date_start = threadr::parse_unix_time(date_start, tz = tz),
           date_end = threadr::parse_unix_time(date_end, tz = tz),
           auto_correlation = as.logical(auto_correlation),
           deseason = as.logical(deseason))
  
  # Drop variable 
  if (!date_insert) df <- select(df, -date_insert)
  
  return(df)
  
}
