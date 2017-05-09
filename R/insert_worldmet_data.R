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
#' @importFrom magrittr %>%
#' 
#' @export
insert_worldmet_data <- function(con, site, start, end = NA,
                                 verbose = TRUE) {
  
  # Ceiling round
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Get look-up tables
  df_processes <- import_processes(con, type = "minimal") %>% 
    dplyr::select(process, 
                  site,
                  variable)
  
  # Small look-up table just to decode site_code
  df_sites_small <- import_sites(con) %>% 
    dplyr::select(site,
                  site_code)
  
  # Get data
  if (verbose) message("Downloading data with worldmet...")
  df <- download_noaa(site, start, end)
  
  # Insert if there is data
  if (nrow(df) > 0) {
    
    # Make longer and join, inner join will only keep those in processes table
    df <- df %>% 
      dplyr::left_join(df_sites_small, by = "site_code") %>% 
      dplyr::select(-site_code) %>% 
      tidyr::gather(variable, value, -date, -site, na.rm = TRUE) %>% 
      dplyr::mutate(date_end = date + 3599,
                    date = as.integer(date),
                    date_end = as.integer(date_end),
                    summary = 1L,
                    validity = NA) %>% 
      dplyr::inner_join(df_processes, by = c("site", "variable")) 
    
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
#' @importFrom magrittr %>%
#' 
#' @export
download_noaa <- function(site, start = 1990, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(lubridate::today())
  
  # Download
  # importNOAA is not vectorised over site
  df <- plyr::ldply(site, worldmet::importNOAA, year = start:end, hourly = TRUE)

  # Just in case, may not be needed
  closeAllConnections()
  
  # Drop and rename
  df <- df %>% 
    dplyr::select(-usaf,
                  -wban,
                  -station,
                  -lat,
                  -lon,
                  -elev) %>% 
    dplyr::rename(site_code = code)
  
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
