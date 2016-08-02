#' Function to get observations from \strong{openair's} \code{importKCL} 
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
#' \code{`sites`} table. The default is \code{"openair:importKCL"}. 
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
insert_kings_college_data <- function(con, site, start, end = NA,
                                      data_source = "openair:importKCL", 
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
  
  # Filter to data source
  df_sites <- df_sites[df_sites$data_source == data_source, ]
  
  # Filter to service too
  if (service) 
    df_sites <- df_sites[df_sites$service == 1 & !is.na(df_sites$service), ]
  
  # A bit slow
  if (verbose) message("Downloading data from Kings College London...")
  df <- download_kings_college(unique(df_sites$site), start, end)
  
  # Make longer and join, inner join will only keep those in processes table
  df <- df %>% 
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


#' Function to get observations which are served by Kings College London and 
#' accessed with \strong{openair}. 
#' 
#' @author Stuart K. Grange
#' 
#' @param Site Site code, for example \code{"rb1"}. 
#' 
#' @param start Start year.
#' 
#' @param end End year. 
#' 
#' @import dplyr
#' 
#' @export
download_kings_college <- function(site, start, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Get data
  df <- tryCatch({
    
    suppressWarnings(
      quiet(
        openair::importKCL(site = site, year = start:end)
      )
    )
    
  }, error = function(e) {
    
    # Return nothing
    NULL
    
  })
  
  # Ensure everything is closed
  closeAllConnections()
  
  if (!is.null(df)) {
  
    # Clean site things
    df$site <- NULL
    names(df) <- ifelse(names(df) == "code", "site", names(df))
    df$site <- stringr::str_to_lower(df$site)
    
    # Fix other names
    names(df) <- ifelse(names(df) == "nv2.5", "nv25", names(df))
    names(df) <- ifelse(names(df) == "v2.5", "v25", names(df))
    
  } else {
    
    # Empty
    df <- data.frame()
    
  }
  
  # Return
  df
  
}