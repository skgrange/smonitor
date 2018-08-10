#' Function to calculate \code{date_start}, \code{date_end}, and 
#' \code{observation_count} for processes and then update the \code{`processes`}
#' table for an \strong{smonitor} database.
#' 
#' @param con Database connection. 
#' 
#' @param process A process vector to update. Default is \code{NA} which will 
#' cause the database to update all process keys. 
#' 
#' @param na.rm Should missing values (\code{NA}/\code{NULL}) be omited from the
#' aggregation functions? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible. 
#' 
#' @export
update_process_spans <- function(con, process = NA, na.rm = FALSE) {
  
  # Calculate process spans
  df <- calculate_process_spans(
    con, 
    process = process, 
    na.rm = na.rm
  ) 
  
  if (nrow(df) != 0) {
    
    df <- df %>% 
      mutate(date_start = stringr::str_replace_na(date_start), 
             date_end = stringr::str_replace_na(date_end))
    
    # Build update statements
    sql <- stringr::str_c(
      "UPDATE processes
     SET date_start=", df$date_start, 
      ",date_end=", df$date_end, 
      ",observation_count=", df$observation_count,
      " WHERE process=", df$process, ""
    )
    
    # No quotes for NULL
    sql <- stringr::str_replace_all(sql, "NA", "NULL")
    
    # Clean
    sql <- threadr::str_trim_many_spaces(sql)
    
    # Update variables to be null before insert, only when all processes are 
    # Done
    if (is.na(process[1])) {
      
      databaser::db_execute(
        con, 
        "UPDATE processes SET date_start = NULL, date_end = NULL"
      )
      
    } 
    
    # Use statements
    databaser::db_execute(con, sql)
    
  }
  
  # No return
  
}


calculate_process_spans <- function(con, process, na.rm) {
  
  # Build sql
  if (is.na(process[1])) {
    
    sql <- "SELECT process, 
         MIN(date) AS date_start, 
         MAX(date) AS date_end,
         CAST(COUNT(*) AS REAL) AS observation_count
         FROM observations 
         GROUP BY process 
         ORDER BY process"
    
  } else {
    
    sql <- stringr::str_c(
      "SELECT process, 
      MIN(date) AS date_start, 
      MAX(date) AS date_end,
      CAST(COUNT(*) AS REAL) AS observation_count
      FROM observations", 
      " WHERE process IN (", 
      stringr::str_c(process, collapse = ","),
      ") GROUP BY process 
      ORDER BY process"
    )
    
  }
  
  # Omit NULL values
  if (na.rm) {
    
    sql <- stringr::str_replace(
      sql, 
      "FROM observations", 
      " FROM observations WHERE value IS NOT NULL "
    )
    
    # Extra catch for when processes are specified
    sql <- stringr::str_replace(sql, "WHERE process", " AND process ")
    
  }
  
  # Clean query
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Get table
  df <- databaser::db_get(con, sql)
  
  # If no processes are given, do all the processes
  if (is.na(process[1])) {
    
    # sql group by will not return groups with no data
    df_processes <- databaser::db_get(
      con,
      "SELECT process
       FROM processes
       ORDER BY process"
    )

    # Join and add observation counts if missing
    df <- df_processes %>%
      left_join(df, by = "process") %>%
      mutate(
        observation_count = ifelse(
          is.na(observation_count),
          0,
          observation_count)
      )
    
  }
  
  return(df)
  
}
