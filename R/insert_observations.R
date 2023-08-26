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
#' existence in the `processes` table before insert?
#' 
#' @param check_validity Should the validity in \code{df} be tested before 
#' insert? 
#' 
#' @param batch_size Number of rows to insert per batch. Useful for large 
#' inserts. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @param progress If \code{batch_size} is not \code{NA}, should a progress bar 
#' be displayed? 
#' 
#' @return Invisible \code{con}.
#' 
#' @seealso \code{\link{delete_observations}}
#' 
#' @examples 
#' \dontrun{
#' 
#' # Insert observations data
#' insert_observations(con, data_test)
#' 
#' # Insert observations, this time with some messages
#' insert_observations(con, data_test, verbose = TRUE)
#' 
#' }
#' 
#' @export
insert_observations <- function(con, df, check_processes = TRUE, 
                                check_validity = TRUE, batch_size = NA, 
                                verbose = FALSE, progress = FALSE) {
  
  # Get system date
  date_system <- round(lubridate::now())
  
  # Return immediately when input contains no observations
  if (nrow(df) == 0) {
    if (verbose) {
      cli::cli_alert_info(
        "{threadr::cli_date()} Input data has no observations, the database has not been touched..."
      )
    }
    return(invisible(con))
  }
  
  if (verbose) {
    cli::cli_alert_info("{threadr::cli_date()} Formatting input for smonitor insert...")
  }
  
  # Get variables in database table
  df_template <- databaser::db_table_names(con, "observations")
  
  # Drop variables which are not in `observations`
  index <- if_else(names(df) %in% names(df_template), TRUE, FALSE)
  df <- df[, index]
  
  # Convert dates to unix time, before the row binding, otherwise conversion
  # occurs
  if (lubridate::is.POSIXt(df$date)) df$date <- as.numeric(df$date)
  if (lubridate::is.POSIXt(df$date_end)) df$date_end <- as.numeric(df$date_end)
  
  # Order variables and also add variables which do not exist
  df <- bind_rows(df_template, df)
  
  # Add date insert variable
  df$date_insert <- as.numeric(date_system)
  
  # Do some checking
  if (verbose) {
    cli::cli_alert_info(
      "{threadr::cli_date()} Checking smonitor's constraints before insert..."
    )
  }
  
  if (anyNA(df$process)) {
    cli::cli_abort("Missing processes detected, no data inserted.")
  }
  
  # Check process keys for presence in `processes`
  if (check_processes) {
    
    # Do the test
    processes_not_in_processes <- check_process(con, df$process)
    
    # Error if they do not exist
    if (length(processes_not_in_processes) != 0) {
      cli::cli_abort("Processes to be inserted do not exist in `processes` table.")
    }
    
  }
  
  if (anyNA(df$summary)) {
    cli::cli_abort("Missing summaries detected, no data inserted.")
  }
  
  if (anyNA(df$date)) {
    cli::cli_abort("Missing dates detected, no data inserted.")
  }
  
  # Check validity
  if (check_validity) {
    
    # Get unique values
    validity_values <- unique(df$validity)
    
    # Test
    if (!any(validity_values %in% c(NA, -1:3))) {
      cli::cli_abort("Validity contains incorrect values, no data inserted.")
    }
    
  }
  
  if (verbose) {
    cli::cli_alert_info(
      "{threadr::cli_date()} Inserting `{threadr::str_thousands_separator(nrow(df))}` observations into `observations`..."
    )
  }
  
  # Insert into `observations`
  if (!is.na(batch_size) && nrow(df) >= batch_size) {
    
    if (verbose) {
      cli::cli_alert_info(
        "{threadr::cli_date()} Splitting input and inserting in batches..."
      )
    }
    
    # Split into certain sizes
    list_df <- threadr::split_nrow(df, batch_size)
    
    # Insert in pieces
    purrr::walk(
      list_df,
      ~databaser::db_insert(con, "observations", .), 
      .progress = progress
    )
    
  } else {
    # Insert
    databaser::db_insert(con, "observations", df, replace = FALSE)
  }
  
  return(invisible(con))
  
}
