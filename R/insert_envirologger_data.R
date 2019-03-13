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
#' @return Invisible \code{con}.
#' 
#' @export
insert_envirologger_data <- function(con, user, key, station, start, end = NA,
                                     verbose = FALSE) {
  
  # Load look-up tables
  # Sites
  df_sites_look_up <- databaser::db_get(
    con, 
    "SELECT site,
    envirologger_station AS station
    FROM sites
    ORDER BY site"
  )
  
  # Import process keys granular to channel number and sensor combinations
  df_processes <- import_processes(con) %>% 
    filter(service != 0 | is.na(service)) %>% 
    select(process,
           site,
           variable,
           channel_number = envirologger_channel_number,
           sensor_id = envirologger_sensor_id)
  
  # Get observations with API
  if (verbose) message(threadr::date_message(), "Getting new observations...")
  
  df <- envirologgerr::get_envirologger_data(
    user = user, 
    key = key, 
    station = station, 
    start = start, 
    end = end,
    verbose = verbose
  )
  
  if (nrow(df) != 0) {
    
    # Join smonitor site
    df <- df %>% 
      left_join(df_sites_look_up, by = "station") %>% 
      select(-station)
    
    # Store number of observations before joining
    n_row_pre_processes <- nrow(df)
    
    # Only processes in table will be kept
    df <- inner_join(df, df_processes, by = c("site", "channel_number", "sensor_id"))
    
    # Test for equal or fewer observations
    if (nrow(df) > n_row_pre_processes) {
      stop(
        "Process join caused observations to be replicated, some processes are duplicated...", 
        call. = TRUE
      )
    }
    
    # Transform for smonitor
    df <- df %>% 
      mutate(date = as.numeric(date),
             date_end = NA_real_, 
             validity = NA_integer_,
             summary = 0L) %>% 
      select(date,
             date_end,
             process,
             summary,
             validity,
             value)
    
    # Join may drop all observations
    if (nrow(df) > 0) {
      
      # Upsert observations
      delete_observations(con, df, match = "between", verbose = verbose)
      insert_observations(con, df, verbose = verbose)
      
    }
    
  } else {
    
    if (verbose) {
      
      message(
        threadr::date_message(), 
        "No data inserted because API returned no data..."
      )
    
    } 
    
  }
  
  return(invisible(con))
  
}
