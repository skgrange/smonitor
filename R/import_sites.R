#' Function to import \code{`sites`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
#' 
#' @param tz What time zone should the \code{date_start} and \code{date_end}
#' variables be represented as? 
#' 
#' @param correct_integers Should integers be corrected for a SQLite issue? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
#' 
#' @examples 
#' \dontrun{
#' 
#' # Import sites from a smonitor database
#' data_sites <- import_sites(con)
#' 
#' }
#' 
#' @export
import_sites <- function(con, tz = "UTC", correct_integers = TRUE) {
  
  # A simple statement
  sql <- stringr::str_squish(
    "SELECT * 
    FROM sites 
    ORDER BY site"
  )
  
  # Get table
  df <- databaser::db_get(con, sql)
  
  # Bug in SQLite integers and double representation, should be able to drop soon
  if (correct_integers) {
    
    df <- df %>% 
      dplyr::mutate_if(is.double, ~if_else(. == -2147483648, NA_real_, .)) %>% 
      dplyr::mutate_if(is.integer, ~if_else(. == -2147483648, NA_integer_, .))
    
  }
  
  # Parse dates
  df <- df %>% 
    mutate(date_start = suppressWarnings(as.numeric(date_start)),
           date_end = suppressWarnings(as.numeric(date_end)),
           date_start = threadr::parse_unix_time(date_start, tz = tz),
           date_end = threadr::parse_unix_time(date_end, tz = tz))
  
  return(df)
  
}
