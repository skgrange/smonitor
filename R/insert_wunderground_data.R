#' Function to get observations from wunderground and insert them into a 
#' \strong{smonitor} database. 
#' 
#' Site-variable combinations need to be present in the database's process table,
#' otherwise they will be silently filtered and not be inserted. New, downloaded
#' observations will take priority over those stored in the database and old 
#' observations are deleted with this function. 
#' 
#' @param con Database connection. 
#' 
#' @param site Wunderground site code. 
#' 
#' @param start Start date to download and insert. 
#' 
#' @param end End date to download and insert. 
#' 
#' @param validity Should the validity variable be updated? Not implemented yet. 
#' 
#' @param verbose Should the function give messages? Default is \code{FALSE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
insert_wunderground_data <- function(con, site, start, end = NA, validity = FALSE,
                                     verbose = FALSE) {
  
  # For dplyr
  site_vector <- site
  
  # Get look-up table for a join
  df_look <- import_processes(con) %>% 
    filter(site %in% site_vector) %>% 
    select(process,
           site,
           variable)
  
  # Get observations, every site is done separately
  message("Getting new observations...")
  df <- plyr::ldply(site_vector, function(x) 
    sscraper::scrape_wunderground(x, start = start, end = end, verbose = verbose))
  
  if (nrow(df) != 0) {
    
    # Transform and reshape data
    df <- df %>% 
      select(-date, 
             -date_local, 
             -software) %>% 
      rename(date = date_unix) %>% 
      tidyr::gather(variable, value, -date, -site) %>% 
      mutate(date_end = NA, 
             validity = NA,
             summary = 0L)
    
    # To-do validity
    
    # Join processes, only processes in table will be kept, then arrange
    df <- df %>% 
      inner_join(df_look, by = c("site", "variable")) %>% 
      select(date,
             date_end,
             process,
             summary,
             validity,
             value)
    
    if (nrow(df) > 0) {
      
      # Delete observations
      message("Deleting old observations...")
      
      # Does the grouping
      delete_observations(con, df, match = "between", convert = FALSE, 
                          progress = "none")
      
      # Insert
      message("Inserting new observations...")
      insert_observations(con, df)
      
    } else {
      
      message("No data inserted...")
      
    }
    
  } else {
    
    message("No data was returned from the API...")
    
  }
  
}


# No export at the moment
insert_wunderground_data_frame <- function(con, df) {

  if (length(unique(df$site)) != 1) 
    stop("Only one site allowed in data frame.", call. = FALSE)
  
  # Get site
  site_vector <- df$site[1]
  
  # Get look-up table for a join
  df_look <- import_processes(con) %>% 
    filter(site %in% site_vector) %>% 
    select(process,
           site,
           variable)
  
  # Transform and reshape data
  df <- df %>% 
    select(-date, 
           -date_local, 
           -software) %>% 
    rename(date = date_unix) %>% 
    tidyr::gather(variable, value, -date, -site) %>% 
    mutate(date_end = NA, 
           validity = NA,
           summary = 0L)
  
  # Join processes, only processes in table will be kept, then arrange
  df <- df %>% 
    inner_join(df_look, by = c("site", "variable")) %>% 
    select(date,
           date_end,
           process,
           summary,
           validity,
           value)
  
  # Insert
  if (nrow(df) > 0) {
    
    # Delete observations
    message("Deleting old observations...")
    
    # Does the grouping
    delete_observations(con, df, match = "between", convert = FALSE, 
                        progress = "none")
    
    # Insert
    message("Inserting new observations...")
    insert_observations(con, df)
    
  } else {
    
    message("No data inserted...")
    
  }
  
  # No return
  
}
