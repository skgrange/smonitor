#' Function to find start and end dates of a process in an \strong{smonitor}
#' database. 
#' 
#' To-do: Add WHERE clauses for summary and validity and also processes. 
#' 
#' @param con Database connection. 
#' @param tz Time-zone to parse the dates to. 
#' 
#' @author Stuart K.Grange
#' 
#' @import dplyr
#' 
#' @export
calculate_process_spans <- function(con, tz = "UTC") {
  
  # Build sql
  sql <- "SELECT process, 
         MIN(date) AS date_start, 
         MAX(date) AS date_end,
         CAST(COUNT(*) AS TEXT) AS observation_count
         FROM observations 
         GROUP BY process 
         ORDER BY process"
  
  # Get table
  df <- databaser::db_get(con, sql) %>% 
    mutate(date_start = threadr::parse_unix_time(date_start, tz = tz), 
           date_end = threadr::parse_unix_time(date_end, tz = tz))
  
  # Return
  df
  
}
