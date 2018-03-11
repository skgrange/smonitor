#' Function to import the \code{`trend_test`} table from an \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @param date_insert Should the return include the \code{date_insert} variable? 
#' Default is \code{FALSE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @export
import_trend_tests <- function(con, date_insert = FALSE) {
  
  # Check for table
  if (!databaser::db_table_exists(con, "trend_tests"))
    stop("`trend_tests` table does not exist...", call. = FALSE)
  
  # Get table
  df <- databaser::db_get(
    con, 
    "SELECT * 
    FROM trend_tests
    ORDER BY site, 
    variable,
    summary"
  ) %>% 
    mutate(date_insert = threadr::parse_unix_time(date_insert),
           date_start = threadr::parse_unix_time(date_start),
           date_end = threadr::parse_unix_time(date_end),
           significant = as.logical(significant))
  
  if (!date_insert) df$date_insert <- NULL
  
  return(df)
  
}
