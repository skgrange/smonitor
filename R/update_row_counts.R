#' Function to get row counts and insert as a table in a database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param estimate Should the row counts be estimated rather than counted? Only
#' works for PostgreSQL databases.
#'  
#' @param read Should the table be immediately read after being updated? Default
#' is \code{FALSE}. 
#' 
#' @seealso \code{\link{db_count_rows}}
#' 
#' @export
update_row_counts <- function(con, estimate = FALSE, read = FALSE) {
  
  # Get all tables
  table <- databaser::db_list_tables(con)
  
  # But do not do `row_counts` table
  table <- setdiff(table, "row_counts")
  
  # Get counts of all tables
  df <- databaser::db_count_rows(con, table = table, estimate = estimate)
  
  # Insert and always replace table
  databaser::db_insert(con, "row_counts", df, replace = TRUE)
  
  # Reassign df
  if (read) df <- databaser::db_read_table(con, "row_counts")

  # Return
  if (read) df else invisible()

}
