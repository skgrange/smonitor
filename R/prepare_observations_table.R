#' Function to prepare a data frame for insert into the \code{`observation`} 
#' table in a \strong{smonitor} database. 
#' 
#' @param df Data frame.
#' 
#' @param drop Should variables not contained in an \code{`observations`} table 
#' be dropped? 
#' 
#' @param convert Should the variables be coerced into the correct data types? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#'
#' @export
prepare_observations_table <- function(df, drop = FALSE, convert = FALSE) {
  
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
    # value? 
    
  }
  
  return(df)
  
}
