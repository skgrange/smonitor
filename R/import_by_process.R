#' Function to import observational data from a \strong{smonitor} database. 
#' 
#' \code{import_by_process} is considered the primary data importer for 
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
#' Default is \code{FALSE}. 
#' 
#' @param site_name Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param warn Should the functions raise warnings? 
#' 
#' @param print_query Should the SQL query string be printed? 
#' 
#' @return Data frame containing decoded observational data with correct data 
#' types. 
#' 
#' @seealso \code{\link{import_by_site}} for a higher-level importing function
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_by_process <- function(con, process = NA, summary = NA, start = 1969, 
                              end = NA, tz = "UTC", valid_only = TRUE, 
                              date_end = TRUE, date_insert = FALSE, 
                              site_name = TRUE, warn = TRUE, 
                              print_query = FALSE) {
  
  # Check
  if (is.na(process[1])) 
    stop("The `process` argument must be used...", call. = FALSE)
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0)
    end <- end + lubridate::hours(23) + lubridate::minutes(59) + 
    lubridate::seconds(59)
  
  # For sql
  start <- as.numeric(start)
  end <- as.numeric(end)
  process <- stringr::str_c(process, collapse = ",")
  summary <- stringr::str_c(summary, collapse = ",")
  
  # Get table to link processes with sites
  df_processes <- import_by_process_process_table(
    con, 
    process, 
    site_name, 
    print_query
  )
  
  # Check for data
  if (nrow(df_processes) == 0) {
    
    if (warn) {
      
      warning(
        "Process(s) not found in database, no data has been returned...", 
        call. = FALSE
      )
      
    }
    
    # Return empty data frame here
    return(data.frame())
    
  }
  
  # Get observations
  df <- import_by_process_observation_table(
    con, 
    process = process, 
    summary = summary, 
    start = start,
    end = end,
    date_end = date_end, 
    date_insert = date_insert, 
    print_query = print_query
  )
  
  # Check for data
  if (nrow(df) == 0) {
    
    if (warn) {
      
      warning(
        "Database has been queried but no data has been returned...", 
        call. = FALSE
      )
      
    }
    
    # Return empty data frame here
    return(data.frame())
    
  }
  
  # Filter invalid observations, not 0, may move to sql at some point
  if (valid_only) df <- df[is.na(df$validity) | df$validity == 1, ]
  
  # Check for data
  if (nrow(df) == 0) {
    
    if (warn) {
      
      warning("Database contains no valid observations...", call. = FALSE)
      
    }
    
    # Return empty data frame here
    return(data.frame())
    
  }
  
  # Join process and site data
  df <- left_join(df, df_processes, by = "process")
  
  # Parse dates
  if (date_insert) 
    df$date_insert <- threadr::parse_unix_time(df$date_insert, tz = tz)
  
  if (date_end) 
    df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  
  # Arrange
  df <- arrange(df, process, date)
  
  # And make a nice variable order
  df <- select(
    df, 
    dplyr::matches("date_insert"),
    dplyr::matches("date"), 
    dplyr::matches("date_end"), 
    dplyr::matches("site"), 
    dplyr::matches("site_name"), 
    dplyr::matches("variable"), 
    dplyr::matches("process"), 
    dplyr::matches("summary"),
    dplyr::matches("validity"),
    dplyr::matches("value"),
    dplyr::everything()
  )
  
  return(df)
  
}


import_by_process_process_table <- function(con, process, site_name, print_query) {
  
  # Link processes to sites
  sql_processes <- stringr::str_c(
    "SELECT processes.process, 
    processes.site,
    processes.variable,
    sites.site_name
    FROM processes 
    LEFT JOIN sites
    ON processes.site = sites.site
    WHERE process IN (", process, ")"
  )
  
  # Clean
  sql_processes <- threadr::str_trim_many_spaces(sql_processes)
  
  # Message statement to user
  if (print_query) message(stringr::str_c("Processes query: ", sql_processes))
  
  # Get data
  df <- databaser::db_get(con, sql_processes)
  
  # Drop site_name
  if (!site_name) df$site_name <- NULL
  
  return(df)
  
}


import_by_process_observation_table <- function(con, process, summary, start, 
                                                end, date_end, date_insert, 
                                                print_query) {
  
  if (is.na(summary[1])) {
    
    sql_observations <- stringr::str_c(
      "SELECT observations.date_insert, 
      observations.date,
      observations.date_end,
      observations.value,
      observations.process,
      observations.summary,
      observations.validity
      FROM observations
      WHERE observations.process IN (", process, ")
      AND observations.date BETWEEN ", start, " AND ", end
    )
    
  } else {
    
    sql_observations <- stringr::str_c(
      "SELECT observations.date_insert, 
      observations.date,
      observations.date_end,
      observations.value,
      observations.process,
      observations.summary,
      observations.validity
      FROM observations
      WHERE observations.process IN (", process, ")
      AND observations.date BETWEEN ", start, " AND ", end, "
      AND observations.summary IN (", summary, ")"
    )
    
  }
  
  # Drop date_end from query
  if (!date_end) {
    
    sql_observations <- stringr::str_replace(
      sql_observations, 
      "observations.date_end,", 
      ""
    )
    
  }

  # Drop date_insert from query
  if (!date_insert) {
    
    sql_observations <- stringr::str_replace(
      sql_observations, 
      "observations.date_insert,", 
      ""
    )
    
  }
    
  # Clean sql
  sql_observations <- threadr::str_trim_many_spaces(sql_observations)
  
  # Message statement to user
  if (print_query) message(stringr::str_c("Observations query: ", sql_observations))
  
  # Get observations
  df <- databaser::db_get(con, sql_observations)
  
  return(df)
  
}
