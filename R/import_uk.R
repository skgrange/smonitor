#' Function to import UK air quality data from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param site A site code such as \code{"my1"}. 
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
#' @param extra Should the data frame contain extra variables? Default is 
#' \code{FALSE}.
#' 
#' @param heathrow Should Heathrow's meteorological data be added to the data
#' frame? Useful for dealing with London's air quality data. 
#' 
#' @importFrom magrittr %>%
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
      dplyr::filter(site == "hea",
                    variable %in% c("wd", "ws", "temp", "rh", "pressure"))
    
    # Get data, watch that extra is not used here
    df_heathrow <- import_by_process(
      con, process = df_processes_heathrow$process, summary = 1,
      start = start, end = end)
    
  }
  
  # Filter to site input
  df_processes <- df_processes[df_processes$site %in% site, ]
  
  # Get sites' data
  df <-  import_by_process(
    con, process = df_processes$process, summary = 1,
    start = start, end = end)
  
  if (!extra) {
    
    # Spread data
    df <- df %>%
      dplyr::select(-date_insert,
                    -process,
                    -summary) %>%
      tidyr::spread(variable, value)
    
    if (heathrow) {
      
      # Cast without site identifiers
      df_heathrow <- df_heathrow %>%
        dplyr::select(-date_insert,
                      -process,
                      -summary,
                      -site,
                      -site_name) %>%
        tidyr::spread(variable, value)
      
      # Join heathrow's met data
      df <- df %>%
        dplyr::left_join(df_heathrow, by = c("date", "date_end"))
      
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
        dplyr::bind_rows(df_heathrow)
      
    }
    
  }
  
  # Return
  df
  
}
