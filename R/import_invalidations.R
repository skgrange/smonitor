#' Function to import \code{`invalidations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @param tz What time zone should the \code{date_start} and \code{date_end}
#' variables be represented as?  
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @examples 
#' \dontrun{
#' 
#' # Import invalidations from a smonitor database
#' data_invalidations <- import_invalidations(con)
#' 
#' }
#' 
#' @export
import_invalidations <- function(con, tz = "UTC") {
  
  databaser::db_read_table(con, "invalidations") %>%
    mutate(date_start = lubridate::ymd_hms(date_start, tz = tz, truncated = 4),
           date_end = lubridate::ymd_hms(date_end, tz = tz, truncated = 4))
  
}
