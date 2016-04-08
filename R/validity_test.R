#' Function to add a validity variable to a data frame based on date ranges. 
#' 
#' @author Stuart K. Grange. 
#' 
#' @export
validity_test <- function(df, df_look) {

  # Get identifiers
  site <- df$site[1]
  variable <- df$variable[1]
  
  # Filter look up table
  df_look <- df_look[df_look$site == site & df_look$variable == variable, ]
  
  if (nrow(df_look) >= 1) {
    
    # Test ranges
    df$test <- threadr::within_range(df$date, df_look$date_start, df_look$date_end)
    
    # Add validity variable
    df$validity <- ifelse(df$test, 0, NA)
    
    # Drop test
    df$test <- NULL
    
  }
  
  # Return
  df
  
}
