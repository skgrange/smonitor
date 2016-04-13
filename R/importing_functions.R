#' Functions to import source data from a \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param process A process, an integer key. 
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
import_source <- function(con, process, start = 1970, end = NA, tz = "UTC",
                          valid = FALSE) {
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0)
    end <- end + lubridate::hours(23) + lubridate::minutes(59) + lubridate::seconds(59)
  
  # For sql
  start <- as.integer(start)
  end <- as.integer(end)
  
  # For sql
  process <- stringr::str_c(process, collapse = ",")
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date,
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
    " AND observations.summary = 0 OR observations.summary IS NULL
    ORDER BY observations.process, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query database
  df <- threadr::db_get(con, sql)
  
  # Filter invalid observations
  if (valid) df <- df[is.na(df$validity) | df$validity == 1, ]

  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  # Return
  df
  
}


#' Function to import hourly means from a \strong{smonitor} database. 
#'  
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' @param start Start date to import. 
#' @param end End date to import. 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @export
import_hourly_means <- function(con, process, start = 1970, end = NA, tz = "UTC") {
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0)
    end <- end + lubridate::hours(23) + lubridate::minutes(59) + lubridate::seconds(59)
  
  # For sql
  start <- as.integer(start)
  end <- as.integer(end)
  
  # For sql
  process <- stringr::str_c(process, collapse = ",")
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date,
    observations.date_end,
    observations.value,
    observations.process,
    observations.summary,
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
    " AND observations.summary = 1
    ORDER BY observations.process, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  # Return
  df
  
}


#' Function to import daily means from a \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' @param start Start date to import. 
#' @param end End date to import. 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @export
import_daily_means <- function(con, process, start = 1970, end = NA, tz = "UTC") {
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0)
    end <- end + lubridate::hours(23) + lubridate::minutes(59) + lubridate::seconds(59)
  
  # For sql
  start <- as.integer(start)
  end <- as.integer(end)
  
  # For sql
  process <- stringr::str_c(process, collapse = ",")
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date,
    observations.date_end,
    observations.value,
    observations.process,
    observations.summary,
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
    " AND observations.summary = 20
    ORDER BY observations.process, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  # Return
  df
  
}


#' Function to import data from an air quality monitoring data with the 
#' \strong{smonitor} data model.
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' @param summary A summary, an integer key. 
#' @param start Start date to import. 
#' @param end End date to import. 
#' @param tz Time-zone for the dates to be parsed into. Default is 
#' \code{"Etc/GMT-12"}. 
#' 
#' @export
import_nz <- function(con, process, summary, start = 1970, end = NA, tz = "Etc/GMT-12") {
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0)
    end <- end + lubridate::hours(23) + lubridate::minutes(59) + lubridate::seconds(59)
  
  # For sql
  start <- as.integer(start)
  end <- as.integer(end)
  
  # For sql
  process <- stringr::str_c(process, collapse = ",")
  summary <- stringr::str_c(summary, collapse = ",")
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date,
    observations.date_end,
    observations.value,
    observations.process,
    observations.summary,
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
    AND observations.validity IS NOT 0
    ORDER BY observations.process, 
    observations.summary, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  # Return
  df
  
}


#' Function to import process table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_processes <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- threadr::db_get(con, "SELECT processes.*, 
                              sites.site_name
                              FROM processes
                              LEFT JOIN sites
                              ON processes.site = sites.site
                              ORDER BY processes.process")
  
  # Only a few variables
  if (!extra)
    df <- df[, c("process", "site_name", "site", "variable", "period")]
  
  # Return
  df
  
}


#' Function to import aggregation table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_summaries <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- threadr::db_get(con, "SELECT summaries.*,
                              sites.site_name
                              FROM summaries
                              LEFT JOIN sites
                              ON summaries.site = sites.site
                              ORDER BY summaries.process, 
                              summaries.summary")
  
  # Only a few variables
  if (!extra)
    df <- df[, c("process", "summary", "summary_name", "site", "site_name", 
                 "variable", "source", "validity_threshold", "period")]
  
  # Return
  df
  
}
