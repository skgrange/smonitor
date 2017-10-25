#' Function to get observations from the Envirologger API and insert them into a 
#' \strong{smonitor} database. 
#' 
#' Site-variable combinations must to be present in the database's process table,
#' otherwise they will be silently filtered and not be inserted. New, downloaded
#' observations will take priority over those stored in the database and old 
#' observations are deleted with this function. 
#' 
#' @param con Database connection. 
#' 
#' @param user An Envirologger API user-name. 
#' 
#' @param key An Envirologger API key for \code{user}. 
#' 
#' @param station A vector of station codes to download.
#' 
#' @param start Start date to download and insert. 
#' 
#' @param end End date to download and insert. 
#' 
#' @param verbose Should the funciton give messages?
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible, a database insert. 
#' 
#' @export
insert_envirologger_data <- function(con, user, key, station, start, end = NA,
                                     verbose = FALSE) {
  
  # Load look-up tables
  # Sites
  df_sites_look_up <- import_sites(con) %>% 
    select(site,
           station = envirologger_station)
  
  # Process keys
  df_processes <- import_processes(con) %>% 
    filter(service != 0 | is.na(service)) %>% 
    select(process,
           site,
           variable,
           channel_number = envirologger_channel_number)
  
  # Get observations with API
  if (verbose) message("Getting new observations...")
  
  df <- envirologgerr::get_envirologger_data(
    user = user, 
    key = key, 
    station = station, 
    start = start, 
    end = end
  )
  
  if (nrow(df) != 0) {
    
    # Site, not station bitte
    df <- df %>% 
      left_join(df_sites_look_up, by = "station") %>% 
      select(-station)
    
    # Only processes in table will be kept
    df <- df %>% 
      inner_join(df_processes, by = c("site", "channel_number"))
    
    # Transform data frame for smonitor
    df <- df %>% 
      mutate(date = as.numeric(date),
             date_end = NA, 
             validity = NA,
             summary = 0L) %>% 
      select(date,
             date_end,
             process,
             summary,
             validity,
             value)
    
    # Join may drop all observations
    if (nrow(df) > 0) {
      
      # Delete observations
      if (verbose) message("Deleting old observations...")
      
      # Does the grouping
      delete_observations(
        con, 
        df, 
        match = "between", 
        progress = "time"
      )
      
      # Insert
      if (verbose) message("Inserting new observations...")
      insert_observations(con, df)
      
    } else {
      
      if (verbose) message("No data inserted...")
      
    }
    
  } else {
    
    if (verbose) message("No data inserted because API returned no data...")
    
  }
  
  # No return
  
}
