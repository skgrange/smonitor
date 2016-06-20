#' Function to get row counts and insert as a table in a database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param read Should the table be immediately read after being updated? Default
#' is \code{FALSE}. 
#' 
#' @export
update_row_counts <- function(con, read = FALSE) {
  
  # Get counts of all tables
  df <- threadr::db_count_rows(con, table = NA)
  
  # Insert and always overwrite
  threadr::db_insert(con, "row_counts", df, append = FALSE, overwrite = TRUE)
  
  # Reassign df
  if (read) df <- threadr::db_read_table(con, "row_counts")

  # Return
  if (read) df else invisible()

}
