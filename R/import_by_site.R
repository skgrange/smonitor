#' Function to import data from an \code{smonitor} database based on sites. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param site A site code such as \code{"my1"}. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param period Averaging period. Default is \code{"hour"}. 
#' 
#' @param valid_only Should only valid data be returned? Default is \code{FALSE}. 
#' 
#' @param pad Should the time-series be padded to ensure all dates in the 
#' observation period are present? Default is \code{TRUE}. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param spread Should the data frame take the wider format resulting from
#' spreading the data? Default is \code{FALSE}. 
#' 
#' @param europe Should \code{import_processes_europe} be used rather than
#' \code{import_processes}? 
#' 
#' @param date_end Should the return include the \code{date_end} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param date_insert Should the return include the \code{date_insert} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param date_insert Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @import dplyr
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_by_site <- function(con, site, start = 1970, end = NA, period = "hour", 
                           valid_only = FALSE, pad = TRUE, tz = "UTC", 
                           spread = FALSE, europe = FALSE, date_end = TRUE, 
                           date_insert = TRUE, site_name = TRUE) {
  
  # Parse arguments
  site <- stringr::str_trim(site)
  
  # Get process table
  if (europe) {
    
    # Get country codes
    country_code <- stringr::str_sub(site, end = 2)
    country_code <- unique(country_code)
    
    # Get filtered mapping table
    df_processes <- import_processes_europe(con, country_code)
    
    # Europe has no summaries, therefore period can be used
    df_processes <- df_processes[df_processes$site %in% site & 
                                   df_processes$period %in% period, ]
    
    # Summary is not used
    summary <- NA
    
  } else {
    
    # Standard mapping table
    df_processes <- import_processes(con)
    
    # Filter to site and period input
    df_processes <- df_processes[df_processes$site %in% site, ]
    
    # Switch period to integer
    summary <- ifelse(period == "hour", 1, period)
    summary <- ifelse(period == "day", 20, period)
    
  }
  
  # Query database to get sites' data
  df <- import_any(con, process = df_processes$process, summary = summary, 
                   start = start, end = end, tz = tz, valid_only = valid_only,
                   date_end = date_end, date_insert = date_insert, 
                   site_name = site_name)
  
  # Drop all NAs for padding and reshaping
  df <- df %>% 
    filter(!is.na(value))
  
  if (spread) {
    
    # Drop
    if (date_insert) df$date_insert <- NULL
    
    # For time-padding
    if (site_name) site <- c("site", "site_name") else site <- "site"
    
    # Cast data
    df <- tryCatch({
      
      df %>%
        select(-process,
               -summary,
               -validity) %>%
        tidyr::spread(variable, value)
      
    }, error = function(e) {
      
      # # Raise warning
      # warning("Variable names were manipulated for reshaping.", call. = FALSE)
      # 
      # df %>%
      #   mutate(process = stringr::str_pad(process, width = 6, pad = "0"),
      #          variable = stringr::str_c(variable, "_", process)) %>% 
      #   select(-date_insert,
      #          -process,
      #          -summary,
      #          -validity) %>%
      #   tidyr::spread(variable, value)
      
      # Raise warning
      warning("Data has been removed for reshaping...", call. = FALSE)
      
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
      df <- df %>% 
        threadr::time_pad(interval = period, by = site)
      
    }
    
  } else {
    
    if (pad) {
      
      # Pad time-series
      df <- df %>% 
        threadr::time_pad(interval = period, 
          by = c("process", "summary", site, "variable"))
      
    }
    
  }
  
  # Return
  df
  
}
