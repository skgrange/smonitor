#' Function to import \code{`sites`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_sites <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- databaser::db_get(con, "SELECT sites.*
                                FROM sites 
                                ORDER BY site")
  
  # Only a few variables
  if (!extra)
    df <- df[, c("site", "site_name", "latitude", "longitude", "elevation", 
                 "region", "site_type")]
  
  # Return
  df
  
}
