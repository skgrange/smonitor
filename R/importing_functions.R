#' Functions to import source data from a database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' 
#' @export
import_source <- function(con, process) {
  
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
    AND observations.summary = 0
    AND observations.validity IS NOT 0
    ORDER BY observations.process, observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query database
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, "nz")
  df$date_end <- threadr::parse_unix_time(df$date_end, "nz")
  
  # Return
  df
  
}



#' Function to import hourly means from a database.
#'  
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' 
#' @export
import_hourly_means <- function(con, process) {
  
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
    AND observations.summary = 1
    AND observations.validity IS NOT 0
    ORDER BY observations.process, observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, "nz")
  df$date_end <- threadr::parse_unix_time(df$date_end, "nz")
  
  # Return
  df
  
}


#' Function to import daily means from a database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' 
#' @export
import_daily_means <- function(con, process) {
  
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
    AND observations.summary = 20
    AND observations.validity IS NOT 0
    ORDER BY observations.process, observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, "nz")
  df$date_end <- threadr::parse_unix_time(df$date_end, "nz")
  
  # Return
  df
  
}


#' Function to import data from an air quality monitoring data. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' @param process A process, an integer key. 
#' @param summary A summary, an integer key. 
#' 
#' @export
import_nz <- function(con, process, summary) {
  
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
    AND observations.summary IN (", summary, ")
    AND observations.validity IS NOT 0
    ORDER BY observations.process, 
    observations.summary, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, "nz")
  df$date_end <- threadr::parse_unix_time(df$date_end, "nz")
  
  # Return
  df
  
}



#' Function to import process table from database. 
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



#' Function to import aggregation table from database.
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
