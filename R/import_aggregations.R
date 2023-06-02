#' Function to import \code{`aggregations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
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
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "aggregations"))
  
  # Get look-up table
  df <- databaser::db_get(
    con, 
    "SELECT *
    FROM aggregations
    ORDER BY summary"
  )
  
  return(df)
  
}
