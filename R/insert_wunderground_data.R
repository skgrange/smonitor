#' Function to get observations from wunderground and insert them into a 
#' \strong{smonitor} database. 
#' 
#' Site-variable combinations need to be present in the database's process table,
#' otherwise they will be silently filtered and not be inserted. 
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
#' @param verbose Should the function give messages and be chatty? Default is 
#' \code{TRUE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
insert_wunderground_data <- function(con, site, start, end = NA, validity = FALSE,
                                     verbose = TRUE) {
  
  # For dplyr
  site_vector <- site
  
  # Get look-up table for a join
  df_look <- import_processes(con) %>% 
    filter(site %in% site_vector) %>% 
    select(process,
           site,
           variable)
  
  # Get observations, every site is done separately
  df <- plyr::ldply(site_vector, function(x) 
    sscraper::scrape_wunderground(x, start = start, end = end, verbose = verbose))
  
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
  
  # To-do
  # if (validity) {}
  # # Test validity
  # df <- df %>% 
  #   group_by(site,
  #            variable) %>% 
  #   do(validity_test(., data_invalidation)) %>% 
  #   ungroup()
  
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
    
    # Grouping
    plyr::d_ply(df, c("process", "summary"), function(x) 
      delete_observations(con, x, match = "between"), .progress = "time")
    
    # Insert
    message("Inserting new observations...")
    threadr::db_insert(con, "observations", df)
    
  } else {
    
    message("No data inserted...")
    
  }

}
