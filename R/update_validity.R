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
#' @param process A vector of processes. 
#' @param tz Time-zone. 
#' @param progress Progress bar type. Default is \code{"time"}. 
#' 
#' @import dplyr
#' @export
update_validity <- function(con, process, tz = "UTC", delete = "between", 
                            progress = "time") {
  
  # Check
  if (!delete %in% c("between", "all")) 
    stop("'delete' must be 'between' or 'all'.", call. = FALSE)
  
  # Get look-up table
  df_look <- import_invalidation(con, tz = tz) %>% 
    mutate(date_start = as.numeric(date_start),
           date_end = as.numeric(date_end))
  
  # Do for every process
  plyr::l_ply(process, function(x) 
    update_validity_worker(con, x, df_look, delete), .progress = progress)
  
  # No return
  
}


# No export needed
update_validity_worker <- function(con, process, df_look, delete) {
  
  # Get observations
  # Catch is for when database contains no data for a process and gives an error
  df <- tryCatch({
    
    import_any(con, process, summary = 0, start = 1965, end = 2020, 
               valid_only = FALSE) %>% 
      mutate(date = as.numeric(date),
             date_end = as.numeric(date_end))
    
  }, error = function(e) {
    
    # Return no observations
    data.frame()
    
  })
  
  if (nrow(df) != 0) {
    
    # Update validity, look up table is filtered in function
    df <- validity_test(df, df_look)
    
    # Delete old observations
    delete_observations(con, df, match = delete, convert = FALSE, 
                        progress = "none")
    
    # Insert new observations
    insert_observations(con, df)
    
  }
  
  # No return
  
}
