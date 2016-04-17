#' Function to add a validity variable to a data frame based on date ranges. 
#' 
#' \code{validity_test} oes not currently handle groups, but will in the future.
#' It is recommended that \code{dplyr::do} is used for this task.
#' 
#' @param df Data frame to test and add or overwrite a \code{validity} variable. 
#' 
#' @param df_look A look-up data frame containing \code{site}, \code{variable}, 
#' \code{date_start}, and \code{date_end} variables.
#' 
#' @param use_process Use \code{"process"} rather than \code{"site"} and 
#' \code{"variable"}. 
#' 
#' @seealso \code{\link{within_range}}
#' 
#' @author Stuart K. Grange. 
#' 
#' @export
validity_test <- function(df, df_look, use_process = FALSE) {

  # Get keys from table
  if (use_process) {
    
    # Get identifier
    process <- df$process[1]
    
    # Filter look up table
    df_look <- df_look[df_look$process == process, ]
    
  } else {
    
    # Get identifiers
    site <- df$site[1]
    variable <- df$variable[1]
    
    # Filter look up table
    df_look <- df_look[df_look$site == site & df_look$variable == variable, ]
    
  }
  
  if (nrow(df_look) > 0) {
    
    # Test ranges
    df$test <- threadr::within_range(df$date, df_look$date_start, df_look$date_end)
    
    # Add validity variable
    df$validity <- ifelse(df$test, 0, NA)
    
    # Drop test variable
    df$test <- NULL
    
  }
  
  # Return
  df
  
}
