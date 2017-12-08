#' Function to import the \code{`trend_test`} table from an \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @export
import_trend_tests <- function(con) {
  
  # Check for table
  if (!databaser::db_table_exists(con, "trend_tests"))
    stop("`trend_tests` table does not exist...", call. = FALSE)
  
  databaser::db_get(
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
  
}
