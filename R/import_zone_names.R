#' Function to import zone names from \code{`zones`} table in an \strong{smonitor}
#' database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @export
import_zone_names <- function(con) {
  
  sql <- "SELECT zone, name FROM zones"
  
  # Get data
  df <- databaser::db_get(con, sql)
  
  # Return
  df
  
}
