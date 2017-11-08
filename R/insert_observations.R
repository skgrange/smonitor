#' Function to insert observations into the \code{`observations`} in a 
#' \strong{smonitor} database. 
#' 
#' \code{insert_observations} will handle the variable/column order, the date 
#' conversions, add a \code{"date_insert"} variable, and the database insert for 
#' a \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange. 
#' 
#' @param con Database connection.
#' 
#' @param df Data frame containing observations to be inserted. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Invisible, an insert into a database table. 
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
insert_observations <- function(con, df, verbose = FALSE) {
  
  if (verbose) message("Formatting input for smonitor insert...")
  
  # No tbl_df
  df <- threadr::base_df(df)
  
  # Get variables in database table
  variables <- databaser::db_list_variables(con, "observations")
  
  # Drop variables which are not in table
  index <- ifelse(names(df) %in% variables, TRUE, FALSE)
  df <- df[, index]
  
  # Convert dates to unix time, before binding otherwise conversion occurs
  if (lubridate::is.POSIXt(df$date)) df$date <- as.numeric(df$date)
  if (lubridate::is.POSIXt(df$date_end)) df$date_end <- as.numeric(df$date_end)
  
  # Ensure inserting data frame has all variables and are in the correct order
  # Create a data frame with only headers
  df_headers <- read.csv(
    textConnection(stringr::str_c(variables, collapse = ",")), 
    stringsAsFactors = FALSE
  )
  
  # Bind
  # To-do, this is expensive and slow but it handles data-type changes well
  df <- plyr::rbind.fill(df_headers, df)
  
  # Force after binding
  gc()
  
  # Add variable
  date_insert <- lubridate::now()
  date_insert <- round(date_insert)
  df$date_insert <- as.numeric(date_insert)
  
  # Do some checking
  if (verbose) message("Checking smonitor's constraints...")
  
  if (anyNA(df$process)) 
    stop("Missing processes detected, no data inserted...", call. = FALSE)
  
  if (anyNA(df$summary))
    stop("Missing summaries detected, no data inserted...", call. = FALSE)
  
  if (anyNA(df$date))
    stop("Missing dates detected, no data inserted...", call. = FALSE)
  
  # Print a message
  if (verbose) {
    
    list_message <- list(
      message = "Inserting observations...", 
      date_begin = format(date_insert, usetz = TRUE),
      observations = threadr::str_thousands_separator(nrow(df)),
      size = threadr::object_size(df)
    )
    
    # The message
    message(jsonlite::toJSON(list_message, pretty = TRUE, auto_unbox = TRUE))
    
  }
  
  # Insert into database
  databaser::db_insert(con, "observations", df)
  
}
