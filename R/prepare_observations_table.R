#' Function to prepare a data frame for insert into the \code{`observation`} 
#' table in a \strong{smonitor} database. 
#' 
#' @param df Data frame. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#'
#' @export
prepare_observations_table <- function(df) {
  
  # , convert = TRUE
  
  # Build template data frame
  names <- c(
    "date_insert", "date", "date_end", "process", "summary", "validity", 
    "value"
  )
  
  # Make data frame
  df_smonitor <- data.frame(
    matrix(ncol = length(names), nrow = 0)
  )
  
  # Give names
  names(df_smonitor) <- names
  
  # Correct order of variables
  df <- dplyr::bind_rows(df_smonitor, df)
  
  # any(grepl("date", names(df)))
  # any(grepl("date_end", names(df)))
  # any(grepl("process", names(df)))
  # any(grepl("summ", names(df)))
  
  return(df)
  
}
