#' Function to import \code{`sites`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' @param print_query Should the SQL query string be printed? 
#' 
#' @export
import_sites <- function(con, extra = TRUE, print_query = FALSE) {
  
  # Statement
  sql <- "SELECT sites.*
          FROM sites 
          ORDER BY site"
  
  # Clean
  sql <- str_trim_many_spaces(sql)
  
  # Message
  if (print_query) message(sql)
  
  # Get look-up table
  df <- databaser::db_get(con, sql)
  
  # Only a few variables
  if (!extra)
    df <- df[, c("site", "site_name", "latitude", "longitude", "elevation", 
                 "region", "site_type")]
  
  # Return
  df
  
}
