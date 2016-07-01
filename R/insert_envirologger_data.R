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
#' @param server An Envirologger API server code to use for download.
#' 
#' @param start Start date to download and insert. 
#' 
#' @param end End date to download and insert. 
#' 
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
insert_envirologger_data <- function(con, user, key, station, server, 
                                     start, end = NA) {
  
  # Load look-up tables
  # Sites
  df_sites_look_up <- import_sites(con) %>% 
    select(site,
           station = envirologger_station)
  
  # Process keys
  df_processes <- import_processes(con) %>% 
    select(process,
           site,
           variable,
           label = envirologger_label,
           sensor = variable_long)
  
  # Get observations with an API
  message("Getting new observations...")
  
  df <- envirologgerr::get_envirologger_data(
    user = user, key = key, station = station, server = server, start = start, 
    end = end, clean = TRUE)
  
  # Site, not station bitte
  df <- df %>% 
    left_join(df_sites_look_up, by = "station") %>% 
    select(-station)
  
  # Only processes in table will be kept
  df <- df %>% 
    inner_join(df_processes, by = c("site", "label", "sensor", "variable"))

  # Transform data frame for smonitor
  df <- df %>% 
    mutate(date = as.integer(date),
           date_end = NA, 
           validity = NA,
           summary = 0L) %>% 
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
                        progress = "time")
    
    # Insert
    message("Inserting new observations...")
    insert_observations(con, df)
    
  } else {
    
    message("No data inserted...")
    
  }
  
}
