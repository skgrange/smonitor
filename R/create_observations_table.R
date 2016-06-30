#' Function to create \code{`observations`} table for a \strong{smonitor} 
#' database.
#' 
#' @param con Database connection. 
#' 
#' @param index Should an index on the \code{process} variable be created? 
#' Default is \code{TRUE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
create_observations_table <- function(con, index = TRUE) {
  
  # Create new table
  databaser::db_send(con, "CREATE TABLE observations(
                           date_insert INTEGER,
                           date INTEGER,
                           date_end INTEGER,
                           process INTEGER,
                           summary INTEGER,
                           validity INTEGER,
                           value REAL)")
  
  # Add index
  if (index)
    databaser::db_send(con, "CREATE INDEX index_observations_process 
                             ON observations(process)")
  
  # No return
  
}
