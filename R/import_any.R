#' Function to import any observational data from a \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param process A process, an integer key. 
#' 
#' @param summary A summary, an integer key. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param valid Should invalid observations be filtered out? Default is 
#' \code{FALSE}. 
#' 
#' @export
import_any <- function(con, process, summary, start = 1970, end = NA, tz = "UTC",
                       valid = FALSE, extra = TRUE) {
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0)
    end <- end + lubridate::hours(23) + lubridate::minutes(59) + 
    lubridate::seconds(59)
  
  # For sql
  start <- as.integer(start)
  end <- as.integer(end)
  
  # For sql
  process <- stringr::str_c(process, collapse = ",")
  summary <- stringr::str_c(summary, collapse = ",")
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date_insert,
    observations.date,
    observations.date_end,
    observations.value,
    observations.process,
    observations.summary,
    observations.validity,
    processes.site,
    processes.variable, 
    sites.site_name
    FROM observations 
    LEFT JOIN processes 
    ON observations.process = processes.process
    LEFT JOIN sites
    ON processes.site = sites.site
    WHERE observations.process IN (", process, ")
    AND observations.date BETWEEN ", start, " AND ", end, 
    " AND observations.summary IN (", summary, ")
    ORDER BY observations.process, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query database
  df <- threadr::db_get(con, sql)
  
  # Filter invalid observations
  if (valid) df <- df[is.na(df$validity) | df$validity == 1, ]
  
  # Parse dates
  df$date_insert <- threadr::parse_unix_time(df$date_insert, tz = tz)
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  # Drop extras
  if (!extra) {
    
    df$date_insert <- NULL
    df$date_end <- NULL
    
  }
  
  # Return
  df
  
}