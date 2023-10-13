#' Function to prepare a data frame for insert into the \code{`observations`} 
#' table in a \strong{smonitor} database. 
#' 
#' @param df Data frame or tibble. 
#' 
#' @param drop Should variables not contained in an \code{`observations`} table 
#' be dropped? 
#' 
#' @param convert Should the variables be coerced into the correct data types? 
#' 
#' @param as_tibble Should the return be a tibble? 
#' 
#' @param include_date_insert Should the \code{date_insert} variable be included? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame or tibble. 
#'
#' @export
prepare_observations_table <- function(df, drop = FALSE, convert = FALSE,
                                       as_tibble = FALSE, 
                                       include_date_insert = TRUE) {
  
  # Build template data frame
  names <- c(
    "date_insert", "date", "date_end", "process", "summary", "validity", 
    "value"
  )
  
  # Only the variables which are in `observations`, do this before binding
  if (drop) {
    index <- which(names(df) %in% names)
    df <- df[, index]
  }
    
  # Make data frame
  df_smonitor <- data.frame(
    matrix(ncol = length(names), nrow = 0)
  )
  
  # Give names
  names(df_smonitor) <- names
  
  # Correct order of variables
  df <- dplyr::bind_rows(df_smonitor, df)
  
  # Correct data types
  if (convert) {
    df$date_insert <- as.numeric(df$date_insert)
    df$date <- as.numeric(df$date)
    df$date_end <- as.numeric(df$date_end)
    df$process <- as.integer(df$process)
    df$summary <- as.integer(df$summary)
    df$validity <- as.integer(df$validity)
  }
  
  # To tibble
  if (as_tibble) df <- as_tibble(df)
  
  # Drop date_insert if not needed
  if (!include_date_insert) {
    df <- select(df, -date_insert)
  }
  
  return(df)
  
}
