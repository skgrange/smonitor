#' Function to import data from an \strong{smonitor} database based on sites. 
#' 
#' \code{import_by_site} is considered the primary user-focused importing 
#' function while \code{import_any} is the primary lower-level function. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param site A site code such as \code{"my1"}. 
#' 
#' @param variable An optional variable vector. If not used, all variables will
#' be selected and returned.  
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
#' @param europe Should \code{import_processes_europe} be used rather than
#' \code{import_processes}? 
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
#' @seealso \code{\link{import_any}}
#' 
#' @import dplyr
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_by_site <- function(con, site, variable = NA, start = 1970, end = NA, 
                           period = "hour", valid_only = TRUE, pad = TRUE, 
                           tz = "UTC", spread = FALSE, europe = FALSE, 
                           date_end = TRUE, date_insert = FALSE, 
                           site_name = TRUE, print_query = FALSE) {
  
  # Parse arguments
  site <- stringr::str_trim(site)
  variable <- stringr::str_trim(variable)
  
  if (period %in% c("all", "any", "source")) pad <- FALSE
  
  # Get process table
  if (europe) {
    
    # Get country codes
    country_code <- stringr::str_sub(site, end = 2)
    country_code <- unique(country_code)
    
    # Get filtered mapping table
    df_processes <- import_processes_europe(con, country_code, minimal = TRUE)
    
    if (period %in% c("all", "any")) {
      
      # No filtering on period
      df_processes <- df_processes[df_processes$site %in% site, ]
      
    } else {
      
      # Europe has no summaries, therefore period can be used here
      df_processes <- df_processes[df_processes$site %in% site & 
                                     df_processes$period %in% period, ]
      
    }
    
    # Filter to variable too
    if (!is.na(variable[1]))
      df_processes <- df_processes[df_processes$variable %in% variable, ]
    
    # Summary is used differently in Europe database
    summary <- NA
    
  } else {
    
    # Standard mapping table
    df_processes <- import_processes(con)
    
    # Filter to site and period input
    df_processes <- df_processes[df_processes$site %in% site, ]
    
    # Filter to variable
    if (!is.na(variable[1]))
      df_processes <- df_processes[df_processes$variable %in% variable, ]
    
    # Switch period to integer
    summary <- ifelse(period == "source", 0, period)
    summary <- ifelse(period == "hour", 1, summary)
    summary <- ifelse(period == "day", 20, summary)
    
  }
  
  # Check mapping table
  if (nrow(df_processes) == 0) 
    stop("Mapping table contains no entries. Do you need to check the 'site' and 'period' arguments?", call. = FALSE)
  
  # Query database to get sites' data
  df <- import_any(con, process = df_processes$process, summary = summary, 
                   start = start, end = end, tz = tz, valid_only = valid_only,
                   date_end = date_end, date_insert = date_insert, 
                   site_name = site_name, print_query = print_query)
  
  # Drop all NAs for padding and reshaping
  df <- df %>% 
    filter(!is.na(value))
  
  # For time-padding
  if (site_name) site_variables <- c("site", "site_name") else site_variables <- "site"
  
  if (spread) {
    
    # Drop
    if (date_insert) df$date_insert <- NULL
    
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
        threadr::time_pad(interval = period, by = site_variables)
      
    }
    
  } else {
    
    if (pad) {
      
      # Pad time-series
      df <- df %>% 
        threadr::time_pad(interval = period, 
          by = c("process", "summary", site_variables, "variable"))
      
    }
    
  }
  
  # Clean R's names prefix, occurs with hydrocarbon variables
  names(df) <- stringr::str_replace(names(df), "^X", "")
  
  # Return
  df
  
}
