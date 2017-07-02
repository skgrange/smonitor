#' Function to update the \code{date_start}, \code{date_end}, variables in the
#' \code{`processes`} and \code{`sites`} tables in a \strong{smonitor} database. 
#' 
#' \code{update_date_span_variables} will also insert (or replace) the 
#' \code{`row_counts`} table. 
#' 
#' @param con Database connection.
#' 
#' @param tz Time-zone Time-zone for the dates to be represented as. 
#' 
#' @param na.rm Should missing values (\code{NA}/\code{NULL}) be omited from the
#' aggregation functions? 
#' 
#' @param verbose Should the function give messages?
#' 
#' @author Stuart K. Grange
#' 
#' @export
update_date_span_variables <- function(con, tz = "UTC", na.rm = FALSE, 
                                       verbose = FALSE) {
  
  # Do
  if (verbose) message("Updating `processes` table...")
  update_process_spans(con, tz = tz, na.rm = na.rm)
  
  if (verbose) message("Updating `sites` table...")
  update_site_spans(con, tz = tz)
  
  # Also row counts
  # Message text logic
  if (verbose) {
    
    if (databaser::db_table_exists(con, "row_counts")) {
      
      message("Replacing `row_counts` table...")
      
    } else {
      
      message("Inserting a `row_counts` table...")
      
    }
    
  }
  
  # Do
  update_row_counts(con)
  
  # No return
  
}
