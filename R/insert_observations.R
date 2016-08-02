#' Function to insert observations into the \code{`observations`} in a 
#' \strong{smonitor} database. 
#' 
#' \code{insert_observations} will handle the variable/column order, the date 
#' conversions, add a \code{"date_insert"} variable, and the database insert for 
#' a \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection
#' @param df Data frame containing observations to be inserted. 
#' @param message Should a message of counts and size be displayed? 
#' 
#' @examples 
#' \dontrun{
#' 
#' # Insert some data
#' insert_observations(con, data_test)
#' 
#' # Insert some data, with a message
#' insert_observations(con, data_test, message = TRUE)
#' 
#' }
#' 
#' @export
insert_observations <- function(con, df, message = FALSE) {
  
  # No tbl_df
  df <- threadr::base_df(df)
  
  # Get variables in database table
  variables <- databaser::db_list_variables(con, "observations")
  
  # Drop variables which are not in table
  index <- ifelse(names(df) %in% variables, TRUE, FALSE)
  df <- df[, index]
  
  # Convert dates to unix time, before binding otherwise conversion occurs
  if (lubridate::is.POSIXt(df$date)) df$date <- as.integer(df$date)
  if (lubridate::is.POSIXt(df$date_end)) df$date_end <- as.integer(df$date_end)
  
  # Ensure inserting data frame has all variables and are in the correct order
  # Create a data frame with only headers
  variables_collapse <- stringr::str_c(variables, collapse = ",")
  df_headers <- read.csv(textConnection(variables_collapse), 
                         stringsAsFactors = FALSE)
  
  # Bind
  # To-do, this is expensive and slow
  df <- plyr::rbind.fill(df_headers, df)
  
  # Force after binding
  gc()
  
  # Add variable
  df$date_insert <- threadr::sys_unix_time(integer = TRUE)
  
  # Do some checking
  if (any(is.na(df$process))) warning("Missing processes detected...", call. = FALSE)
  if (any(is.na(df$summary))) warning("Missing summaries detected...", call. = FALSE)
  if (any(is.na(df$date))) warning("Missing dates detected...", call. = FALSE)
  
  # Print a message
  if (message) {
    
    df_message <- data.frame(
      message = "Inserting observations...", 
      observations = threadr::str_thousands_separator(nrow(df)),
      size = threadr::object_size(df)
    )
    
    # The message
    message(jsonlite::toJSON(df_message, pretty = TRUE))
    
  }
  
  # Insert
  databaser::db_insert(con, "observations", df)
  
}
