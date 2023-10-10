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
#' @param tz Time zone for the dates to be parsed into. 
#' 
#' @param progress Should a progress bar be displayed? 
#' 
#' @return Tibble. 
#' 
#' @seealso \code{\link{update_process_entries}}
#' 
#' @export
summarise_process_entiries <- function(con, process, tz = "UTC", 
                                       progress = FALSE) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "processes"))
  
  # Message how many processes to evaluate
  if (progress) {
    cli::cli_alert_info(
      "{threadr::cli_date()} `{length(process)}` processes to summarise..."
    )
  }
  
  # Summarie each process in turn
  process %>% 
    purrr::map(
      ~summarise_process_entiries_worker(con, .), .progress = progress
    ) %>% 
    purrr::list_rbind() %>% 
    arrange(process) %>% 
    mutate(
      across(c(date_start, date_end), ~threadr::parse_unix_time(., tz = tz))
    )
  
}


summarise_process_entiries_worker <- function(con, process) {
  
  sql <- glue::glue(
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
