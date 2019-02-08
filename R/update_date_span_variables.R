#' Function to update the \code{date_start}, \code{date_end}, variables in the
#' \code{`processes`} and \code{`sites`} tables in a \strong{smonitor} database. 
#' 
#' \code{update_date_span_variables} will also insert (or replace) the 
#' \code{`row_counts`} table. 
#' 
#' @param con Database connection.
#' 
#' @param na.rm Should missing values (\code{NA}/\code{NULL}) be omited from the
#' aggregation functions? 
#' 
#' @param row_counts Should the \code{`row_counts`} table be updated (or 
#' inserted)? 
#' 
#' @param variables_monitored Should the \code{variable_monitored} vaiable also 
#' be updated? 
#' 
#' @param verbose Should the function give messages?
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
update_date_span_variables <- function(con, na.rm = FALSE, row_counts = FALSE, 
                                       variables_monitored = TRUE, 
                                       verbose = FALSE) {
  
  # Processes table
  if (verbose) message(threadr::str_date_formatted(), ": Updating `processes` table...")
  update_process_spans(con, na.rm = na.rm)
  
  # Sites table
  if (verbose) message(threadr::str_date_formatted(), ": Updating `sites` table...")
  update_site_spans(con, variables_monitored = variables_monitored)
  
  # Also row counts
  if (row_counts) {
    
    # Message text logic
    if (verbose) {
      
      if (databaser::db_table_exists(con, "row_counts")) {
        
        message(threadr::str_date_formatted(), ": Replacing `row_counts` table...")
        
      } else {
        
        message(threadr::str_date_formatted(), ": Inserting a `row_counts` table...")
        
      }
      
    }
    
    # Do
    update_row_counts(con)
    
  }
  
  return(invisible(con))
  
}
