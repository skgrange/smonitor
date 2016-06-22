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
  
  # Get sites' data
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
    
    if (nrow(df) < 1) 
      stop("Database has been queried but no data has been returned.",
           call. = FALSE)
    
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