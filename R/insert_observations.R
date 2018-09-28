#' Function to insert observations into the \code{`observations`} in a 
#' \strong{smonitor} database. 
#' 
#' \code{insert_observations} will handle the variable/column order, the date 
#' conversions, add a \code{"date_insert"} variable, and the database insert for 
#' a \strong{smonitor} database. A number of tests are conducted before insertion
#' is attempted. 
#' 
#' @author Stuart K. Grange. 
#' 
#' @param con Database connection.
#' 
#' @param df Data frame containing observations to be inserted. 
#' 
#' @param check_processes Should the processes in \code{df} be tested for their
#' existance in the `processes` table before insert?
#' 
#' @param batch_size Number of rows to insert per batch. Useful for large 
#' inserts. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Invisible, \code{con}.  
#' 
#' @examples 
#' \dontrun{
#' 
#' # Insert some data
#' insert_observations(con, data_test)
#' 
#' # Insert some data, with some messages
#' insert_observations(con, data_test, verbose = TRUE)
#' 
#' }
#' 
#' @export
insert_observations <- function(con, df, check_processes = TRUE, 
                                batch_size = NA, verbose = FALSE) {
  
  # Get system date
  date_system <- round(lubridate::now())
  
  if (verbose) 
    message(threadr::str_date_formatted(), ": Formatting input for smonitor insert...")
  
  # Get variables in database table
  df_template <- databaser::db_table_names(con, "observations")
  
  # Drop variables which are not in `observations`
  index <- ifelse(names(df) %in% names(df_template), TRUE, FALSE)
  df <- df[, index]
  
  # Convert dates to unix time, before binding otherwise conversion occurs
  if (lubridate::is.POSIXt(df$date)) df$date <- as.numeric(df$date)
  if (lubridate::is.POSIXt(df$date_end)) df$date_end <- as.numeric(df$date_end)
  
  # Order variables and also add variables which do not exist
  df <- bind_rows(df_template, df)
  
  # Add date insert variable
  df$date_insert <- as.numeric(date_system)
  
  # Do some checking
  if (verbose) {
    
    message(
      threadr::str_date_formatted(), 
      ": Checking smonitor's constraints before insert..."
    )
    
  }
  
  if (anyNA(df$process)) 
    stop("Missing processes detected, no data inserted...", call. = FALSE)
  
  # Check process keys for presence in `processes`
  if (check_processes) {
    
    # Do the test
    processes_not_in_processes <- check_process(con, df$process)
    
    # Error if they do not exist
    if (length(processes_not_in_processes) != 0) {
      
      stop(
        "Processes to be inserted do not exist in `processes` table...", 
        call. = FALSE
      )
      
    }
    
  }
  
  if (anyNA(df$summary))
    stop("Missing summaries detected, no data inserted...", call. = FALSE)
  
  if (anyNA(df$date))
    stop("Missing dates detected, no data inserted...", call. = FALSE)
  
  # Insert into observations
  if (is.na(batch_size)) {
    
    if (verbose) {
      
      message(
        threadr::str_date_formatted(), 
        ": Inserting ", 
        threadr::str_thousands_separator(nrow(df)), 
        " observations into `observations`..."
      )
      
    }
    
    # Insert
    databaser::db_insert(con, "observations", df)
    
  } else {
    
    if (verbose) 
      message(threadr::str_date_formatted(), ": Splitting input...")
    
    # Split
    list_df <- threadr::split_nrow(df, batch_size)
    
    if (verbose) {
      
      message(
        threadr::str_date_formatted(), 
        ": Inserting ", 
        threadr::str_thousands_separator(nrow(df)), 
        " observations into `observations` with ", 
        length(list_df), 
        " batches..."
      )
      
    }
    
    # Insert in pieces
    purrr::walk(list_df, ~databaser::db_insert(con, "observations", .))
    
  }
  
  invisible(con)
  
}
