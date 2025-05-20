#' Function to get row counts and insert as a table in a database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param estimate Should the row counts be estimated rather than counted? Only
#' works for PostgreSQL databases.
#' 
#' @seealso \code{\link[databaser]{db_count_rows}}
#' 
#' @return Invisible, tibble. 
#' 
#' @export
update_row_counts <- function(con, estimate = FALSE) {
  
  .Deprecated("db_count_rows_insert", package = "databaser")
  
  # Get all tables
  table <- databaser::db_list_tables(con)
  
  # But do not do `row_counts` table
  table <- setdiff(table, "row_counts")
  
  # Get counts of all tables
  df <- databaser::db_count_rows(con, table = table, estimate = estimate)
  
  # Insert and always replace table
  databaser::db_insert(con, "row_counts", df, replace = TRUE)
  
  # Read table
  df <- databaser::db_read_table(con, "row_counts")
  
  return(invisible(df))

}
