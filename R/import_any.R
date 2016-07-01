#' Function to import observational data from a \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param process A process, an integer key. 
#' 
#' @param summary A summary, an integer key. If summary is \code{NA}, then only 
#' the \code{process} will be used in the \code{WHERE} clause. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param valid_only Should invalid observations be filtered out? Default is 
#' \code{TRUE}. 
#' 
#' @export
import_any <- function(con, process, summary = NA, start = 1970, end = NA, 
                       tz = "UTC", valid_only = TRUE) {
  
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
  process <- stringr::str_c(process, collapse = ",")
  
  if (is.na(summary)) {
    
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
      " ORDER BY observations.process, 
      observations.date")
    
  } else {
    
  # For sql
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
    
  }
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query database
  df <- databaser::db_get(con, sql)
  
  # Filter invalid observations, not 0, may move to sql at some point
  if (valid_only) df <- df[is.na(df$validity) | df$validity == 1, ]
  
  # Parse dates
  df$date_insert <- threadr::parse_unix_time(df$date_insert, tz = tz)
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  # Return
  df
  
}
