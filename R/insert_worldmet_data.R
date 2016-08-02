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
#' @param data_source \code{data_source} variable to use as a filter on the 
#' \code{`sites`} table. The default is \code{"openair:importNOAA"}. 
#' 
#' @param service Should a \code{service} variable be used to filter sites? 
#' If \code{TRUE}, the service value should be \code{1} to indicate service by
#' this function. 
#' 
#' @param verbose Should the function give messages and be chatty? Default is 
#' \code{TRUE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
insert_worldmet_data <- function(con, site, start, end = NA,
                                 data_source = "worldmet:importNOAA", 
                                 service = TRUE, verbose = TRUE) {
  
  # Ceiling round
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Get look-up tables
  df_processes <- import_processes(con, type = "minimal") %>% 
    select(process, 
           site,
           variable)
  
  df_sites <- import_sites(con) %>% 
    select(site,
           site_code, 
           service,
           data_source) 
  
  # Small look-up table just to decode site_code
  df_sites_small <- df_sites %>% 
    select(site,
           site_code)

  # Filter to data source
  df_sites <- df_sites[df_sites$data_source == data_source, ]
  
  # Filter to service too
  if (service) 
    df_sites <- df_sites[df_sites$service == 1 & !is.na(df_sites$service), ]
  
  # A bit slow
  if (verbose) message("Downloading data with worldmet...")
  
  df <- download_noaa(unique(df_sites$site_code), start, end)
  
  # Make longer and join, inner join will only keep those in processes table
  df <- df %>% 
    left_join(df_sites_small, by = "site_code") %>% 
    select(-site_code) %>% 
    gather(variable, value, -date, -site, na.rm = TRUE) %>% 
    mutate(date_end = date + 3599,
           date = as.integer(date),
           date_end = as.integer(date_end),
           summary = 1L,
           validity = NA) %>% 
    inner_join(df_processes, by = c("site", "variable")) 
  
  # Insert if there is data
  if (nrow(df) > 0) {
    
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
#' @import dplyr
#' 
#' @export
download_noaa <- function(site, start = 1990, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Download
  # importNOAA is not vectorised over site
  df <- plyr::ldply(site, worldmet::importNOAA, year = start:end, hourly = TRUE)
  # df <- worldmet::importNOAA(code = site, year = start:end, hourly = TRUE)
  
  # Just in case, may not be needed
  closeAllConnections()
  
  # Drop and rename
  df <- df %>% 
    select(-usaf,
           -wban,
           -station,
           -lat,
           -lon,
           -elev) %>% 
    rename(site_code = code)
  
  # Fix names
  # Other dirty names will still be present
  names(df) <- ifelse(names(df) == "ceil_hgt", "ceiling_height", names(df))
  names(df) <- ifelse(names(df) == "air_temp", "temp", names(df))
  names(df) <- ifelse(names(df) == "atmos_pres", "pressure", names(df))
  names(df) <- ifelse(names(df) == "RH", "rh", names(df))
  names(df) <- ifelse(names(df) == "cl", "cloud_cover", names(df))
  
  # Return
  df
  
}
