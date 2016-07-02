#' Function to calculate hourly means quickly for an \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' @param process A vector of processes. 
#' @param start Start date to import. 
#' @param end End date to import. 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
calculate_hourly_means_fast <- function(con, process, start, end, tz = "UTC") {
  
  # Get all processes' source data
  df <- import_any(con, process, summary = 0, start = start, end = end, 
                   valid_only = TRUE, tz = tz)
  
  # Create lookup table for future joining
  df_look <- df %>% 
    distinct(process,
             site, 
             variable)
  
  # Prepare then reshape
  df <- df %>% 
    select(date,
           date_end, 
           value, 
           site,
           variable) %>% 
    distinct(date,
             site,
             variable,
             .keep_all = TRUE) %>% 
    spread(variable, value) %>% 
    arrange(site, 
            date)
  
  # Do the aggregation
  df_agg <- df %>% 
    openair::timeAverage(avg.time = "hour", data.thresh = 0, type = "site") %>% 
    ungroup() %>% 
    mutate(date_end = date + 3599,
           site = as.character(site))
  
  # Back to normalised data and add process key again
  df_agg <- df_agg %>% 
    gather(variable, value, -date, -date_end, -site) %>% 
    left_join(df_look, by = c("site", "variable")) %>% 
    mutate(summary = 1L)
  
  # Delete
  delete_observations(con, df_agg, match = "between", convert = TRUE, 
                      progress = "none")
  
  # Insert
  insert_observations(con, df_agg)
  
}
