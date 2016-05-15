#' Function to import source data from a \strong{smonitor} database. 
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
import_hourly_means <- function(con, process, start = 1970, end = NA, tz = "UTC", 
                                extra = TRUE) {
  
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
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date_insert, 
    observations.date,
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
    AND observations.validity IS NOT 0
    ORDER BY observations.process, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)
  
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
import_daily_means <- function(con, process, start = 1970, end = NA, tz = "UTC",
                               extra = TRUE) {
  
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
  
  # Build statement
  sql <- stringr::str_c(
    "SELECT observations.date_insert,
    observations.date,
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
    AND observations.validity IS NOT 0
    ORDER BY observations.process, 
    observations.date")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- threadr::db_get(con, sql)

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
import_nz <- function(con, process, summary, start = 1970, end = NA, 
                      tz = "Etc/GMT-12", extra = TRUE) {
  
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


#' Function to import \code{`processes`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_processes <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- threadr::db_get(con, "SELECT processes.*, 
                              sites.site_name, 
                              sites.region, 
                              sites.site_type
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


#' Function to import \code{`summaries`} table from a \strong{smonitor} database. 
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



#' Function to import \code{`sites`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_sites <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- threadr::db_get(con, "SELECT sites.*
                              FROM sites")
  
  # Only a few variables
  if (!extra)
    df <- df[, c("site", "site_name", "latitude", "longitude", "elevation", 
                 "region", "site_type")]
  
  # Return
  df
  
}


#' Function to import \code{`aggregations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_aggregations <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- threadr::db_get(con, "SELECT aggregations.*
                              FROM aggregations")
  
  # Only a few variables
  if (!extra) df <- df[, c("summary", "summary_name")]
  
  # Return
  df
  
}


#' Function to import \code{`invalidations`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' @param tz What time-zone should the dates be in? Default is \code{"UTC"}. 
#' 
#' @import dplyr
#' 
#' @export
import_invalidation <- function(con, tz = "UTC") {
  
  threadr::db_read_table(con, "invalidations") %>%
    mutate(date_start = ymd_hm(date_start, tz = tz),
           date_end = ymd_hm(date_end, tz = tz))
  
}


#' Function to import UK air quality data from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param site A site code such as \code{"my1"}. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param extra Should the data frame contain extra variables? Default is 
#' \code{FALSE}.
#' 
#' @param heathrow Should Heathrow's meteorological data be added to the data
#' frame? Useful for dealing with London's air quality data. 
#' 
#' @import dplyr
#' @import tidyr
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_uk <- function(con, site, start = 1970, end = NA, tz = "UTC", 
                      extra = FALSE, heathrow = FALSE) {
  
  # Get process table
  df_processes <- import_processes(con)
  
  # Get heathrow's met data if needed
  if (heathrow) {
    
    df_processes_heathrow <- df_processes %>% 
      filter(site == "hea",
             variable %in% c("wd", "ws", "temp", "rh", "pressure"))
    
    # Get data, watch that extra is not used here
    df_heathrow <- import_hourly_means(con, df_processes_heathrow$process, start, 
                                       end, tz, extra = TRUE)
    
  }
  
  # Filter to site input
  df_processes <- df_processes[df_processes$site %in% site, ]
  
  # site data
  df <- import_hourly_means(con, df_processes$process, start, end, tz,
                            extra = TRUE)
  
  if (!extra) {
    
    # Cast data
    df <- df %>%
      select(-date_insert,
             -process,
             -summary) %>%
      spread(variable, value)
    
    if (heathrow) {
      
      # Cast without site identifiers
      df_heathrow <- df_heathrow %>%
        select(-date_insert,
               -process,
               -summary,
               -site,
               -site_name) %>%
        spread(variable, value)
      
      # Join heathrow's met data
      df <- df %>%
        left_join(df_heathrow, by = c("date", "date_end"))
      
    }
    
    # Pad time-series
    df <- df %>% 
      threadr::time_pad(interval = "hour", by = c("site", "site_name"))
    
  } else {
    
    if (heathrow) {
      
      # Just bind
      df <- df %>%
        bind_rows(df_heathrow)
      
    }
    
  }

  # Return
  df
  
}
