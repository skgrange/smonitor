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
#' 
#' @param process A vector of processes. 
#' 
#' @param summary Summary to invalidate. This will usually be \code{0} for source
#' data. 
#' 
#' @param tz Time-zone. 
#' 
#' @param delete Type of delete-match to use for dates.
#' 
#' @param progress Progress bar type. Default is \code{"time"}.
#' 
#' @export
update_validity <- function(con, process, summary = 0, tz = "UTC", 
                            delete = "between", progress = "time") {
  
  # Check
  if (!delete %in% c("between", "all")) 
    stop("'delete' must be 'between' or 'all'.", call. = FALSE)
  
  if (!length(summary) == 1) 
    stop("Only one summary can be used.", call. = FALSE)
  
  # Get look-up table
  df_look <- import_invalidations(con, tz = tz) %>% 
    mutate(date_start = as.numeric(date_start),
           date_end = as.numeric(date_end))
  
  # Filter
  df_look <- df_look[df_look[, "process"] %in% process, ]
  
  # Check again
  if (nrow(df_look) == 0) 
    stop("Processes have no entries in the `invalidation` table.", call. = FALSE)
  
  # Do for every process
  plyr::l_ply(process, function(x) 
    update_validity_worker(
      con, 
      process = x, 
      summary = summary, 
      df_look = df_look, 
      delete = delete), 
    .progress = progress
  )
  
  # No return
  
}


# No export needed
update_validity_worker <- function(con, process, summary, df_look, delete) {
  
  # Get observations
  # Catch is for when database contains no data for a process and gives an error
  df <- tryCatch({
    
    import_by_process(con, process, summary = summary, start = 1965, end = 2020, 
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
