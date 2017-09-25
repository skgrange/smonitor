#' Function to import \code{`aggregations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @export
import_aggregations <- function(con) {
  
  # Get look-up table
  df <- databaser::db_get(
    con, 
    "SELECT *
    FROM aggregations"
  )
  
  return(df)
  
}
