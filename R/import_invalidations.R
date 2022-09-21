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
#' @return Tibble. 
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
  
  stopifnot(databaser::db_table_exists(con, "invalidations"))
  
  # Get data
  df <- databaser::db_get(
    con, 
    "SELECT * 
    FROM invalidations
    ORDER BY process,
    date_start"
  )
  
  # Older databases used strings for dates, but they should be integers like
  # all other dates
  if (inherits(df$date_start, "integer")) {
    df <- df %>% 
      mutate(
        across(c("date_start", "date_end"), threadr::parse_unix_time, tz = tz)
      )
  } else {
    df <- df %>% 
      mutate(
        across(
          c("date_start", "date_end"), lubridate::ymd_hms, truncated = 4, tz = tz
        )
      )
  }
  
  return(df)
  
}
