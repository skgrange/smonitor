#' Function to import data from an \strong{smonitor} database based on sites. 
#' 
#' \code{import_by_site} is considered the primary user-focused importing 
#' function while \code{import_by_process} is the primary lower-level function. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param site A vector of sites. 
#' 
#' @param variable An optional variable vector. If not used, all variables will
#' be returned.  
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param period Averaging period. Default is \code{"hour"}. \code{period} can
#' also take the value \code{"any"} which will return all periods, but 
#' \code{pad} argument will be ignored.
#' 
#' @param valid_only Should only valid data be returned? Default is \code{TRUE}. 
#' 
#' @param pad Should the time-series be padded to ensure all dates in the 
#' observation period are present? Default is \code{TRUE}. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param spread Should the data frame take the wider format resulting from
#' spreading the data? Default is \code{FALSE}. 
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
#' @param print_query Should the SQL query string be printed? 
#' 
#' @seealso \code{\link{import_by_process}}
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_by_site <- function(con, site, variable = NA, start = 1970, end = NA, 
                           period = "hour", valid_only = TRUE, pad = TRUE, 
                           tz = "UTC", spread = FALSE, date_end = TRUE,
                           date_insert = FALSE, site_name = TRUE, 
                           print_query = FALSE) {
  
  # Parse arguments
  site <- stringr::str_trim(site)
  site <- threadr::str_sql_quote(site)
  
  variable <- stringr::str_trim(variable)
  variable <- threadr::str_sql_quote(variable)
  
  period <- stringr::str_trim(period)
  period <- stringr::str_replace_all(period, " ", "_")
  
  # Switch period to integer
  summary <- ifelse(period == "source", 0, period)
  summary <- ifelse(period %in% c("fifteen_minute", "15_minute", "15_min"), 15, summary)
  summary <- ifelse(period == "hour", 1, summary)
  summary <- ifelse(period == "day", 20, summary)
  
  # Time padder requires a string for seq
  if (summary == 15) {
    
    interval_pad <- "15 min"
    
  } else {
    
    interval_pad <- period
    
  }
  
  # Does not make sense to pad data in these situations
  if (period %in% c("all", "any", "source")) pad <- FALSE
  
  # Get process keys
  # Build sql, only really need process here
  sql_processes <- stringr::str_c(
    "SELECT process,
     site,
     variable
     FROM processes
     WHERE site IN (", site, ")"
  )
  
  # Add variable as where clause too
  if (!is.na(variable[1])) {
    
    sql_processes <- stringr::str_c(
      sql_processes, 
      " AND variable IN (", variable, ")"
    )
    
  }
  
  # Clean statement
  sql_processes <- threadr::str_trim_many_spaces(sql_processes)
  
  # Get process keys
  df_processes <- databaser::db_get(con, sql_processes)
  
  # Check mapping table
  if (nrow(df_processes) == 0) {
    
    warning(
      "Check the 'site' and 'period' arguments, processes are not available...", 
      call. = FALSE
    )
    
    return(data.frame())
    
  }
  
  # Query database to get sites' data
  df <- import_by_process(
    con,
    process = df_processes$process, 
    summary = summary, 
    start = start, 
    end = end, 
    tz = tz, 
    valid_only = valid_only,
    date_end = date_end, 
    date_insert = date_insert, 
    site_name = site_name, 
    print_query = print_query
  )
  
  # Drop all NAs for padding and reshaping
  df <- df[!is.na(df$value), ]
  
  # Groups for time-padding
  if (site_name) {
    
    site_variables <- c("site", "site_name")  
    
  } else {
    
    site_variables <- "site"
    
  }
  
  
  if (spread) {
    
    # Drop
    if (date_insert) df$date_insert <- NULL
    
    # Cast data
    df <- tryCatch({
      
      df %>%
        select(-process,
               -summary,
               -validity) %>%
        spread(variable, value)
      
    }, error = function(e) {
      
      # Raise warning
      warning(
        "Data has been removed to honour 'spread' argument...", 
        call. = FALSE
      )
      
      df %>%
        distinct(date,
                 site,
                 variable, 
                 .keep_all = TRUE) %>% 
        select(-process,
               -summary,
               -validity) %>%
        tidyr::spread(variable, value)
      
    })
    
    if (pad) {
      
      # Pad time-series
      df <- threadr::time_pad(df, interval = interval_pad, by = site_variables)
      
    }
    
  } else {
    
    if (pad) {
      
      # Pad time-series
      df <- threadr::time_pad(
        df, 
        interval = interval_pad, 
        by = c("process", "summary", site_variables, "variable")
      )
      
    }
    
  }
  
  # Clean R's names prefix, occurs with hydrocarbon variables
  names(df) <- stringr::str_replace(names(df), "^X", "")
  
  return(df)
  
}
