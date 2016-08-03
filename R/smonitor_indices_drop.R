#' Function to drop indices from an smonitor database, usually used before a 
#' batch insert. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @export
smonitor_indices_drop <- function(con) {
  
  # Drop
  databaser::db_send(con, "DROP INDEX index_observations_process")
  
  # databaser::db_send(con, "ALTER TABLE observations 
  #                          DROP CONSTRAINT observations_summary_fkey")
  
  databaser::db_send(con, "ANALYZE observations")
  
  # No return
  
}


#' Function to create indices from an smonitor database, usually used after a 
#' batch insert. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @rdname smonitor_indices_drop
#' @export
smonitor_indices_create <- function(con) {
  
  # Create
  # databaser::db_send(con, "ALTER TABLE observations 
  #                          ADD CONSTRAINT observations_summary_fkey 
  #                          FOREIGN KEY (summary) REFERENCES aggregations")
  
  databaser::db_send(con, "CREATE INDEX index_observations_process 
                           ON observations(process)")
  
  databaser::db_send(con, "ANALYZE observations")
  
  # No return
  
}
