#' Function to import \code{`invalidations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' @param tz What time-zone should the dates be in? Default is \code{"UTC"}. 
#' 
#' @export
import_invalidations <- function(con, tz = "UTC") {
  
  databaser::db_read_table(con, "invalidations") %>%
    mutate(date_start = lubridate::ymd_hm(date_start, tz = tz, truncated = 3),
           date_end = lubridate::ymd_hm(date_end, tz = tz, truncated = 3))
  
}


#' @export
import_invalidation <- function(con, tz = "UTC") {

  # Message
  .Deprecated("import_invalidation", package = "importr")

  # Use function
  import_invalidations(con, tz = "UTC")

}
