#' Function to update validity variable in a \code{`observations`} table in a 
#' \strong{smonitor} database. 
#' 
#' \code{update_validity} does not really update the validity variable, it queries
#' the database, re-tests, deletes the old observations and inserts the re-tested
#' table. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process Process, an integer key. 
#' 
#' @import dplyr
#' @export
update_validity <- function(con, process) {
  
  # Get look-up table
  df_look <- threadr::db_read_table(con, "invalidations") %>% 
    mutate(date_start = lubridate::ymd_hm(date_start, tz = "UTC"),
           date_end = lubridate::ymd_hm(date_end, tz = "UTC"),
           date_start = as.numeric(date_start),
           date_end = as.numeric(date_end))
  
  # Get observations
  df <- import_source(con, process, start = 1970, end = 2020, valid = FALSE) %>% 
    mutate(date = as.numeric(date),
           date_end = as.numeric(date_end))
  
  # Update validity
  df <- validity_test(df, df_look)
  
  # Get variables for insert
  df <- df %>% 
    select(date,
           date_end,
           process, 
           summary,
           validity,
           value)
  
  # Delete old observations
  delete_observations(con, df, match = "between")
  
  # Insert new observations
  threadr::db_insert(con, "observations", df)
  
  # No return
  
}
