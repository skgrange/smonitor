#' Function to update the \code{date_start}, \code{date_end}, variables in the
#' \code{`processes`} and \code{`sites`} tables in a \strong{smonitor} database. 
#' 
#' \code{update_date_span_variables} will also insert (or replace) the 
#' \code{`row_counts`} table. 
#' 
#' @param con \strong{smonitor} database connection. 
#' 
#' @param na.rm Should missing values (\code{NA}/\code{NULL}) be omited from the
#' aggregation functions? 
#' 
#' @param row_counts Should the \code{`row_counts`} table be updated (or 
#' inserted)? 
#' 
#' @param verbose Should the function give messages?
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
update_date_span_variables <- function(con, na.rm = FALSE, row_counts = FALSE, 
                                       verbose = FALSE) {
  
  # Processes table
  if (verbose) {
    cli::cli_alert_info("{threadr::cli_date()} Updating `processes` table...")
  }
  update_process_spans(con, na.rm = na.rm)
  
  # Sites table
  if (verbose) {
    cli::cli_alert_info("{threadr::cli_date()} Updating `sites` table...")
  }
  update_site_spans(con)
  
  # Also row counts
  if (row_counts) {
    
    # Message text logic
    if (verbose) {
      cli::cli_alert_info("{threadr::cli_date()} Updating `row_counts` table...")
    }
    
    # Do the updating or replacing
    databaser::db_count_rows_insert(con, table = "row_counts")
    
  }
  
  return(invisible(con))
  
}
