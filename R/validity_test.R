#' Function to add a validity variable to a data frame based on date ranges. 
#' 
#' @param df Data frame to test and add or overwrite a \code{validity} variable. 
#' 
#' @param df_look A look-up data frame containing \code{process}, 
#' \code{date_start}, and \code{date_end} variables.
#' 
#' @param valid_value For values which are not invalid, \emph{i.e.} they fail 
#' the date range test, what value should they take? Default is \code{NA}, but
#' \code{1} could be appropriate too.  
#' 
#' @seealso \code{\link{within_range}}
#' 
#' @author Stuart K. Grange. 
#' 
#' @export
validity_test <- function(df, df_look, valid_value = NA) {

  # Get keys from table
  # Get identifier
  process <- df$process[1]
  
  # Filter look up table
  df_look <- df_look[df_look$process == process, ]
  
  if (nrow(df_look) > 0) {
    
    # Test ranges
    df$test <- threadr::within_range(df$date, df_look$date_start, df_look$date_end)
    
    # Add validity variable, if within range, invalidate
    df$validity <- ifelse(df$test, 0, valid_value)
    
    # Drop test variable
    df$test <- NULL
    
  }
  
  # Return
  df
  
}
