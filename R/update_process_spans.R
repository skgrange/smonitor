#' Function to calculate \code{date_start}, \code{date_end}, and 
#' \code{observation_count} for processes and then update the \code{`processes`}
#' table for an \strong{smonitor} database.
#' 
#' @param con Database connection. 
#' 
#' @param tz Time-zone to parse the dates to.
#' 
#' @param na.rm Should missing values (\code{NA}/\code{NULL}) be omited from the
#' aggregation functions? 
#' 
#' @author Stuart K. Grange
#' 
#' @export
update_process_spans <- function(con, tz = "UTC", na.rm = FALSE) {
  
  # Calculate process spans
  df <- calculate_process_spans(con, tz = tz, na.rm = na.rm) %>% 
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
  databaser::db_execute(con, sql)
  
  # No return
  
}


calculate_process_spans <- function(con, tz, na.rm) {
  
  # Build sql
  sql <- "SELECT process, 
         MIN(date) AS date_start, 
         MAX(date) AS date_end,
         CAST(COUNT(*) AS TEXT) AS observation_count
         FROM observations 
         GROUP BY process 
         ORDER BY process"
  
  # Omit NULL values
  if (na.rm) {
    
    sql <- stringr::str_replace(
      sql, "FROM observations", " FROM observations WHERE value IS NOT NULL ")
    
  }
  
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
