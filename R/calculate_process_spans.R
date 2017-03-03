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
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Get table
  df <- databaser::db_get(con, sql) %>% 
    mutate(date_start = threadr::parse_unix_time(date_start, tz = tz), 
           date_end = threadr::parse_unix_time(date_end, tz = tz))
  
  # sql group by will not return groups with no data
  df_processes <- databaser::db_get(con, "SELECT process 
                                          FROM processes 
                                          ORDER BY process")
  
  # Join and add observation counts if missing
  df <- df_processes %>% 
    left_join(df, by = "process") %>% 
    mutate(observation_count = ifelse(
      is.na(observation_count), 0, observation_count))
  
  # Return
  df
  
}
