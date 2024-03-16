#' Function to calculate process summaries in a \strong{smonitor} database. 
#' 
#' \code{summarise_process_entiries} will calculate \code{date_start}, 
#' \code{date_end}, \code{n_all}, and \code{n} variables that can be used with 
#' \code{\link{update_process_entries}} to update a \code{processes} table.
#' 
#' @author Stuart K. Grange
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param process A vector of processes. 
#' 
#' @param batch_size Number of processes to summarise per batch. This can be 
#' useful when large numbers of processes are to be summaried. 
#' 
#' @param tz Time zone for the dates to be parsed into. 
#' 
#' @param progress Should a progress bar be displayed? 
#' 
#' @return Tibble. 
#' 
#' @seealso \code{\link{update_process_entries}}
#' 
#' @export
summarise_process_entiries <- function(con, process, batch_size = NA, tz = "UTC", 
                                       progress = FALSE) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "processes"))
  
  # Message how many processes to evaluate
  if (progress) {
    cli::cli_alert_info(
      "{threadr::cli_date()} `{length(process)}` processes to summarise..."
    )
  }
  
  if (is.na(batch_size)) {
    # Summarise each process in turn or one-by-one
    df <- process %>% 
      purrr::map(
        ~summarise_process_entiries_worker(con, .), .progress = progress
      ) %>% 
      purrr::list_rbind() %>% 
      arrange(process) %>% 
      mutate(
        across(c(date_start, date_end), ~threadr::parse_unix_time(., tz = tz))
      )
  } else {
    
    # Split process vector into a list
    list_processes <- threadr::split_nrow(process, batch_size)
    
    # Message how many queries will be sent to the database
    if (progress) {
      cli::cli_alert_info(
        "{threadr::cli_date()} Processes will be summarised in `{length(list_processes)}` batches..."
      )
    }
    
    # Summarise processes in batches
    df <- list_processes %>% 
      purrr::map(
        ~summarise_process_entiries_multiple_worker(con, process = .),
        .progress = progress
      ) %>% 
      purrr::list_rbind() %>% 
      mutate(
        across(c(date_start, date_end), ~threadr::parse_unix_time(., tz = tz))
      )
    
  }
  
  return(df)
  
}


summarise_process_entiries_worker <- function(con, process) {
  
  # Build a sql statement that will
  sql <- stringr::str_glue(
    "SELECT process, 
    MIN(date) AS date_start,
    MAX(date) AS date_end,
    COUNT(*) AS n_all,
    SUM(CASE WHEN value IS NULL THEN 0 ELSE 1 END) AS n
    FROM observations
    WHERE process = {process}"
  )
  
  # Get data, if the process is truly empty, the process key needs to be added 
  # because the database returns nothing
  df <- databaser::db_get(con, sql) %>% 
    mutate(process = if_else(is.na(process), !!process, process))
  
  return(df)
  
}


summarise_process_entiries_multiple_worker <- function(con, process) {
  
  # Create a tibble with all passed processes
  df_process <- tibble(process = as.integer(process))
  
  # Build sql statement with a group by clause
  sql <- stringr::str_glue(
    "SELECT process, 
    MIN(date) AS date_start,
    MAX(date) AS date_end,
    COUNT(*) AS n_all,
    SUM(CASE WHEN value IS NULL THEN 0 ELSE 1 END) AS n
    FROM observations
    WHERE process IN ({stringr::str_c(process, collapse = ',')})
    GROUP BY process"
  )
  
  # Query database and ensure all passed processes are present
  df <- databaser::db_get(con, sql) %>% 
    dplyr::full_join(df_process, by = join_by(process)) %>% 
    mutate(n_all = if_else(is.na(n), 0L, n_all))
  
  return(df)
  
}
