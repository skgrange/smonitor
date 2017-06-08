#' Function to get observations from \strong{worldmet's} \code{importNOAA} 
#' function and insert them into a \strong{smonitor} database. 
#' 
#' Site-variable combinations need to be present in the database's process table,
#' otherwise they will be silently filtered and not be inserted. 
#' 
#' @param con Database connection. 
#' 
#' @param site Site code. 
#' 
#' @param start Start year to download and insert. 
#' 
#' @param end End year to download and insert. 
#' 
#' @param verbose Should the function give messages and be chatty? Default is 
#' \code{TRUE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
insert_worldmet_data <- function(con, site, start, end = NA,
                                 verbose = TRUE) {
  
  # Ceiling round
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Get look-up tables
  df_processes <- import_processes(con, type = "minimal") %>% 
    select(process, 
           site,
           variable)
  
  # Small look-up table just to decode site_code
  df_sites_small <- import_sites(con) %>% 
    select(site,
           site_code)
  
  # Get data
  if (verbose) message("Downloading data with worldmet...")
  df <- download_noaa(site, start, end)
  
  # Insert if there is data
  if (nrow(df) > 0) {
    
    # Make longer and join, inner join will only keep those in processes table
    df <- df %>% 
      left_join(df_sites_small, by = "site_code") %>% 
      select(-site_code) %>% 
      tidyr::gather(variable, value, -date, -site, na.rm = TRUE) %>% 
      mutate(date_end = date + 3599,
             date = as.integer(date),
             date_end = as.integer(date_end),
             summary = 1L,
             validity = NA) %>% 
      inner_join(df_processes, by = c("site", "variable")) 
    
    # Delete observations
    if (verbose) message("Deleting old observations...")
    delete_observations(con, df, match = "between", convert = FALSE, 
                        progress = "none")
    
    # Insert
    if (verbose) message("Inserting new observations...")
    insert_observations(con, df)
    
  } else {
    
    message("No data inserted...")
    
  }
  
  # No return
  
}


#' Function to get observations from NOAA's Integrated Surface Database (ISD)
#' with \code{worldmet}. 
#' 
#' @author Stuart K. Grange
#' 
#' @param Site Site code, for example \code{"037720-99999"}. 
#' 
#' @param start Start year. 
#' 
#' @param end End year. 
#' 
#' @export
download_noaa <- function(site, start = 1990, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(lubridate::today())
  
  # Build mapping data frame
  df_map <- expand.grid(
    site,
    start:end
  )
  
  names(df_map) <- c("site", "year")
  
  # Get
  df <- df_map %>% 
    rowwise() %>% 
    do(download_noaa_worker(.$site, .$year)) %>% 
    ungroup()
  
  # Clean
  if (nrow(df) != 0) {
    
    # Drop and rename
    df <- df %>% 
      select(-dplyr::matches("usaf"),
             -dplyr::matches("wban"),
             -dplyr::matches("station"),
             -dplyr::matches("lat"),
             -dplyr::matches("lon"),
             -dplyr::matches("elev")) %>% 
      rename(site_code = code)
    
    # Fix names
    # Other dirty names will still be present
    names(df) <- ifelse(names(df) == "ceil_hgt", "ceiling_height", names(df))
    names(df) <- ifelse(names(df) == "air_temp", "temp", names(df))
    names(df) <- ifelse(names(df) == "atmos_pres", "pressure", names(df))
    names(df) <- ifelse(names(df) == "RH", "rh", names(df))
    names(df) <- ifelse(names(df) == "cl", "cloud_cover", names(df))
    
  }
  
  return(df)
  
}


download_noaa_worker <- function(site, year) {
  
  # message(
  #   to_json(
  #     list(
  #       site = site,
  #       year = year
  #     )
  #   )
  # )
  
  # Get data
  df <- worldmet::importNOAA(code = site, year = year)
  
  # Just in case, may not be needed
  closeAllConnections()
  
  return(df)
  
}
