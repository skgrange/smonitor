#' Function to calculate \code{date_start}, \code{date_end}, and 
#' \code{observation_count} for processes and then update the \code{`processes`}
#' table for an \strong{smonitor} database.
#' 
#' @param con Database connection. 
#' 
#' @param process A process vector to update. Default is \code{NA} which will 
#' cause the database to update all process keys. 
#' 
#' @param by_process Should the database observations spans be calculated 
#' individually for each process? 
#' 
#' @param na.rm Should missing values (\code{NA}/\code{NULL}) be omited from the
#' aggregation functions? 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
update_process_spans <- function(con, process = NA, by_process = FALSE, 
                                 na.rm = TRUE, verbose = FALSE) {
  
  # Parse input
  if (!is.na(process[1])) {
    process <- process %>% 
      unique() %>% 
      sort()
  }
  
  if (is.na(process) && verbose) {
    message(
      threadr::date_message(), 
      "All processes will be updated, this can be a slow process..."
    )
  }
  
  if (verbose) {
    message(
      threadr::date_message(), 
      "Calculating process spans..."
    )
  }
  
  # Calculate process spans
  df <- calculate_process_spans(
    con, 
    process = process, 
    by_process = by_process,
    na.rm = na.rm
  ) 
  
  if (nrow(df) != 0) {
    
    if (verbose) {
      message(threadr::date_message(), "Updating database...")
    }
    
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
    ) %>% 
      stringr::str_replace_all("NA", "NULL") %>% 
      stringr::str_squish()
    
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
    
  } else {
    if (verbose) {
      message(threadr::date_message(), "No database updates done...")
    }
  }
  
  return(invisible(con))
  
}


calculate_process_spans <- function(con, process, by_process, na.rm) {
  
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
    
    if (!by_process) {
      
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
      
    } else {
      
      sql <- stringr::str_c(
        "SELECT process, 
        MIN(date) AS date_start, 
        MAX(date) AS date_end,
        CAST(COUNT(*) AS REAL) AS observation_count
        FROM observations", 
        " WHERE process=", 
        process
      )
      
    }
    
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
  sql <- stringr::str_squish(sql)
  
  # Get table
  if (length(sql) == 1) {
    df <- databaser::db_get(con, sql)
  } else{
    df <- purrr::map_dfr(sql, ~databaser::db_get(con, .x))
  }
  
  # sql group by will not return groups with no data
  # Could be enhanced to minimise query database less, but need another logic
  # switch
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
      observation_count = if_else(
        is.na(observation_count),
        0,
        observation_count)
    )
  
  if (!is.na(process[1])) df <- filter(df, process %in% !!process)
    
  return(df)
  
}
