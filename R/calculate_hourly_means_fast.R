#' Function to calculate hourly means quickly for an \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @param process A vector of processes. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param na.rm Should \code{NA}s be dropped from the summaries and not inserted
#' into the database. 
#' 
#' @param verbose Should the function give messages? 
#'  
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
calculate_hourly_means_fast <- function(con, process, start, end, tz = "UTC",
                                        na.rm = FALSE, verbose = FALSE) {
  
  # Get all processes' source data
  if (verbose) message("Querying database for source data...")
  df <- import_any(con, process, summary = 0, start = start, end = end, 
                   valid_only = TRUE, tz = tz)
  
  # Prepare then reshape
  if (verbose) message("Do the aggregation...")
  
  df <- df %>% 
    mutate(variable = stringr::str_c(variable, process, sep = ";")) %>% 
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
    mutate(process = stringr::str_split_fixed(variable, pattern = ";", n = 2)[, 2],
           process = as.integer(process),
           variable = stringr::str_split_fixed(variable, pattern = ";", n = 2)[, 1],
           summary = 1L)
  
  # Drop NAs
  if (na.rm) {
    
    df_agg <- df_agg %>% filter(!is.na(value))
    
  }
  
  # Delete
  progress_delete <- ifelse(verbose, "time", "none")
  
  if (progress_delete == "time") 
    message("Deleting old observations...")
  
  delete_observations(con, df_agg, match = "between", convert = TRUE, 
                      progress = progress_delete)
  
  # Insert
  insert_observations(con, df_agg, message = verbose)
  
}
