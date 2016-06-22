#' Function to import data from an \code{smonitor} database based on sites. 
#' 
#' @param con Database connection. 
#' 
#' @param site A site code such as \code{"my1"}. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param period Averaging period. 
#' 
#' @param valid Should only valid data be returned? 
#' 
#' @param pad Should the time-series be padded to ensure all dates in the 
#' observation period are present? 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param extra Should the data frame contain extra variables? Default is 
#' \code{FALSE}.
#' 
#' @import dplyr
#' @import tidyr
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_by_site <- function(con, site, start = 1970, end = NA, period = "hour", 
                           valid = TRUE, pad = TRUE, tz = "UTC", extra = TRUE) {
  
  # site = "ididcot3"; start = 1970; end = NA; tz = "UTC"; period = "hour"; 
  # valid = TRUE
  
  # Check arguments
  if (length(period) > 1) stop("'period' can only take one value", call. = FALSE)
  
  # Get process table
  df_processes <- import_processes(con)
  
  # Filter to site input
  df_processes <- df_processes[df_processes$site %in% site & df_processes$period == period, ]
  
  # message(jsonlite::toJSON(df_processes, pretty = TRUE))
  
  # Switch for period
  summary <- ifelse(period == "hour", 1, NA)
  summary <- ifelse(summary == "day", 20, summary)
  summary <- ifelse(summary == "week", 90, summary)
  summary <- ifelse(summary %in% c("variable", "var"), 91, summary)
  summary <- ifelse(summary == "month", 92, summary)
  summary <- ifelse(summary == "fortnight", 93, summary)
  
  # Get sites' data, note that the extra argument is not used here
  df <- import_any(con, process = df_processes$process, summary = summary, 
                   start = start, end = end, tz = tz, valid = valid)
  
  # Drop all NAs
  df <- df %>% 
    filter(!is.na(value))
  
  # Check return
  if (nrow(df) < 1) 
    stop("Database has been queried but no data has been returned.",
         call. = FALSE)
  
  if (!extra) {
    
    # Cast data
    df <- tryCatch({
      
      df %>%
        select(-date_insert,
               -process,
               -summary,
               -validity) %>%
        spread(variable, value)
      
    }, error = function(e) {
      
      # Raise warning
      warning("Variables were manipulated for reshaping.", call. = FALSE)
      
      df %>%
        mutate(process = stringr::str_pad(process, width = 6, pad = "0"),
               variable = stringr::str_c(variable, "_", process)) %>% 
        select(-date_insert,
               -process,
               -summary,
               -validity) %>%
        spread(variable, value)
      
    })
      
    if (pad) {
      
      # Pad time-series
      df <- df %>% 
        threadr::time_pad(interval = period, by = c("site", "site_name"))
      
    }
    
  } else {
    
    if (pad) {
      
      # Pad time-series
      df <- df %>% 
        threadr::time_pad(interval = period, 
          by = c("process", "summary", "site", "site_name", "variable"))
      
    }
    
  }
  
  # Return
  df
  
}
