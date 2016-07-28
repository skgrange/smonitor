#' Function to import \code{`aggregations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_aggregations <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- databaser::db_get(con, "SELECT aggregations.*
                          FROM aggregations")
  
  # Only a few variables
  if (!extra) df <- df[, c("summary", "summary_name")]
  
  # Return
  df
  
}
