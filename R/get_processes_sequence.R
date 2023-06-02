#' Function to generate a sequence of process keys and test if they exist in
#' an \strong{smonitor} database. 
#' 
#' \code{get_processes_sequence} is useful in determining if groups of processes
#' have been deleted. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param only_outstanding Should the return be filtered to only contain the 
#' processes that do not exist in the database? 
#' 
#' @return Tibble. 
#' 
#' @export
get_processes_sequence <- function(con, only_outstanding = FALSE) {
  
  # Check if table exists
  stopifnot(databaser::db_table_exists(con, "processes"))
  
  # Get current processes
  df_processes_current <- databaser::db_get(
    con,
    "SELECT DISTINCT process
    FROM processes
    ORDER BY process"
  )
  
  # Create a sequence of processes based on min and max values
  process_sequence = seq(
    min(df_processes_current$process),
    max(df_processes_current$process),
    by = 1L
  )
  
  # Add to tibble and test if processes exist
  df <- tibble(
    process = !!process_sequence,
    current = process %in% df_processes_current$process
  )
  
  # Filter table to only include the outstanding processes
  if (only_outstanding) {
    df <- filter(df, !current)
  }
  
  return(df)
  
}
