#' Function to import \code{`processes`} table from a \strong{smonitor} database. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param tz What time zone should the \code{date_start} and \code{date_end}
#' variables be represented as? 
#' 
#' @param print_query Should the SQL query be printed?
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @export
import_processes <- function(con, tz = "UTC", print_query = FALSE) {
  
  # Get everything from processes and things from sites
  sql <- "SELECT processes.*, 
          sites.site_name, 
          sites.region, 
          sites.country,
          sites.site_type
          FROM processes
          LEFT JOIN sites
          ON processes.site = sites.site
          ORDER BY processes.process"
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Message
  if (print_query) message(sql)
  
  # Query
  df <- databaser::db_get(con, sql)
  
  # Parse dates
  df$date_start <- suppressWarnings(as.numeric(df$date_start))
  df$date_end <- suppressWarnings(as.numeric(df$date_end))
  
  df$date_start <- threadr::parse_unix_time(df$date_start, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  return(df)
  
}
