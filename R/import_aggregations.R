#' Function to import \code{`aggregations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @examples 
#' \dontrun{
#' 
#' # Import aggregations from a smonitor database
#' data_aggregations <- import_aggregations(con)
#' 
#' }
#' 
#' @export
import_aggregations <- function(con) {
  
  # Get look-up table
  df <- databaser::db_get(
    con, 
    "SELECT *
    FROM aggregations
    ORDER BY summary"
  )
  
  return(df)
  
}
