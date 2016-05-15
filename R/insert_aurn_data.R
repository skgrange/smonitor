#' Function to get observations from wunderground and insert them into a 
#' \strong{smonitor} database. 
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
#' @import dplyr
#' 
#' @export
insert_aurn_data <- function(con, site, start, end = NA, verbose = TRUE) {
  
  # site <- c("my1", "hea")
  
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Build look-up table
  df_sites <- import_sites(con) %>% 
    select(site,
           site_code, 
           source)
  
  # Join site info needed for logic determining which function to use for input
  df_processes <- import_processes(con) %>% 
    left_join(df_sites, by = "site")
  
  # Filter and select
  df_processes <- df_processes[df_processes$site %in% site, 
    c("process", "site", "variable", "source", "site_code")]
  
  # openair processes
  df_processes_openair <- df_processes %>% 
    filter(source == "openair")
  
  # worldmet processes
  df_processes_worldmet <- df_processes %>% 
    filter(source == "worldmet")
  
  # Smaller look-up table for site_code to site
  df_processes_worldmet_sites <- df_processes_worldmet %>% 
    select(site, 
           site_code) %>% 
    distinct(site_code)
  
  # Get observations
  if (nrow(df_processes_openair) > 0) {
    
    if (verbose) message("Downloading data with openair...")
    df <- download_aurn(unique(df_processes_openair$site), start, end)
    
  } else {
    
    # Placemaker
    df <- NULL
    
  }
  
  if (nrow(df_processes_worldmet) > 0) {
    
    # A bit slow
    if (verbose) message("Downloading data with worldmet...")
    df_worldmet <- download_noaa(unique(df_processes_worldmet$site_code), 
                                 start, end)
    
    # Decode site_code
    df_worldmet <- df_worldmet %>% 
      left_join(df_processes_worldmet_sites, by = "site_code") %>% 
      select(-site_code)
    
  } else {
    
    # Placemaker
    df_worldmet <- NULL
    
  }
  
  # Bind both tables
  df <- bind_rows(df, df_worldmet)
  
  # Make longer and join, inner join will only keep those in processes table
  df <- df %>% 
    gather(variable, value, -date, -site, na.rm = TRUE) %>% 
    mutate(site = str_to_lower(site),
           date_end = date + 3599,
           date = as.integer(date),
           date_end = as.integer(date_end),
           summary = 1L,
           validity = NA) %>% 
    inner_join(df_processes, c("site", "variable")) %>% 
    select(-source, 
           -site_code)
  
  if (nrow(df) > 0) {
    
    # Delete observations
    message("Deleting old observations...")
    
    # Grouping
    plyr::d_ply(df, c("process", "summary"), function(x) 
      delete_observations(con, x, match = "between"), .progress = "time")
    
    # Insert
    message("Inserting new observations...")
    insert_observations(con, df)
    
  } else {
    
    message("No data inserted...")
    
  }
  
}


#' Function to get observations from the United Kingdom's Automatic Urban and 
#' Rural Network (AURN) with \code{openair}. 
#' 
#' @param site Site code, for example \code{"yk11"}. 
#' @param start Start year. 
#' @param end End year. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
download_aurn <- function(site, start = 1990, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  suppressWarnings(
    quiet(
      df <- openair::importAURN(site, year = start:end)
    )
  )
  
  # An issue with openair function when files are missing
  closeAllConnections()
  
  # Clean site things
  df$site <- NULL
  names(df) <- ifelse(names(df) == "code", "site", names(df))
  df$site <- stringr::str_to_lower(df$site)
  
  # Fix other names
  names(df) <- ifelse(names(df) == "pm2.5", "pm25", names(df))
  names(df) <- ifelse(names(df) == "nv2.5", "nv25", names(df))
  names(df) <- ifelse(names(df) == "v2.5", "v25", names(df))
  
  # 
  df <- threadr::arrange_left(df, c("date", "site"))
  
  # Return
  df
  
}


#' Function to get observations from NOAA's Integrated Surface Database (ISD)
#' with \code{worldmet}. 
#' 
#' @author Stuart K. Grange
#' 
#' @param Site Site code, for example \code{"037720-99999"}. 
#' @param start Start year. 
#' @param end End year. 
#' 
#' @import dplyr
#' 
#' @export
download_noaa <- function(site, start = 1990, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Download
  df <- worldmet::importNOAA(code = site, year = start:end, hourly = TRUE)
  
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
  names(df) <- ifelse(names(df) == "ceil_hgt", "ceiling_height", names(df))
  names(df) <- ifelse(names(df) == "air_temp", "temp", names(df))
  names(df) <- ifelse(names(df) == "atmos_pres", "pressure", names(df))
  names(df) <- ifelse(names(df) == "RH", "rh", names(df))
  names(df) <- ifelse(names(df) == "cl", "cloud_cover", names(df))
  # Other dirty names will still be present
  
  # Return
  df
  
}
