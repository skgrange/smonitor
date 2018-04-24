#' Function to import observational data from a \strong{smonitor} database. 
#' 
#' \code{import_any} is considered the primary data importer for 
#' \strong{smonitor}. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param process A vector of processes.
#' 
#' @param summary A vector of summaries. If summary is \code{NA} (the default), 
#' then only the \code{process} will be used in the \code{WHERE} clause. 
#' 
#' @param start What is the start date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and
#' will floor-rounded. 
#' 
#' @param end What is the end date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and 
#' will be ceiling-rounded. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param valid_only Should invalid observations be filtered out? Default is 
#' \code{TRUE}. Valid observations are considered to be those with the validity
#' variable being \code{1} or missing (\code{NULL} or \code{NA}).  
#' 
#' @param date_end Should the return include the \code{date_end} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param date_insert Should the return include the \code{date_insert} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param site_name Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param print_query Should the SQL query string be printed? 
#' 
#' @return Data frame containing decoded observataion data with correct data 
#' types. 
#' 
#' @seealso \code{\link{import_by_site}}
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_any <- function(con, process, summary = NA, start = 1970, end = NA, 
                       tz = "UTC", valid_only = TRUE, date_end = TRUE, 
                       date_insert = FALSE, site_name = TRUE, 
                       print_query = FALSE) {
  
  # Message
  .Defunct("import_by_process", package = "smonitor")
  
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
  
  # Drop date_insert
  if (!date_end)
    sql <- stringr::str_replace(sql, "observations.date_end,", "")
  
  # Drop date_insert
  if (!date_insert)
    sql <- stringr::str_replace(sql, "observations.date_insert,", "")
  
  # Drop site_name, watch the lack of comma here, therefore another catch is 
  # needed
  if (!site_name) {
    
    sql <- stringr::str_replace(sql, "sites.site_name", "")
    sql <- stringr::str_replace(sql, "processes.variable,", "processes.variable")
    
  }
  
  # Clean statement
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Message statement to user
  if (print_query) message(sql)
  
  # Query database
  df <- databaser::db_get(con, sql)
  
  # Check for data
  if (nrow(df) == 0) {
    
    warning("Database has been queried but no data has been returned.", 
            call. = FALSE)
    
    # Return empty data frame here
    return(data.frame())
    
  }
  
  # Filter invalid observations, not 0, may move to sql at some point
  if (valid_only) df <- df[is.na(df$validity) | df$validity == 1, ]
  
  # Parse dates
  # Inserted date
  if (date_insert) 
    df$date_insert <- threadr::parse_unix_time(df$date_insert, tz = tz)
  
  if (date_end) 
    df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  
  return(df)
  
}
