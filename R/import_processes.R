#' Function to import \code{`processes`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
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
#' # Import processes from a smonitor database
#' data_processes <- import_processes(con)
#' 
#' }
#' 
#' @export
import_processes <- function(con, tz = "UTC") {
  
  # Get everything from processes and things from sites
  sql <- "
    SELECT processes.*, 
    sites.site_name, 
    sites.region, 
    sites.country,
    sites.site_type
    FROM processes
    LEFT JOIN sites
    ON processes.site = sites.site
    ORDER BY processes.process
    " %>% 
    stringr::str_squish()
  
  # Query
  df <- databaser::db_get(con, sql) %>% 
    mutate(date_start = parse_numeric_dates(date_start, tz = tz),
           date_end = parse_numeric_dates(date_end, tz = tz))
  
  return(df)
  
}


parse_numeric_dates <- function(x, tz) {
  
  # Warning suppression is for when elments are missing
  suppressWarnings(
    x %>% 
      as.numeric(.) %>% 
      threadr::parse_unix_time(tz = tz
  )
  
)
  
}
