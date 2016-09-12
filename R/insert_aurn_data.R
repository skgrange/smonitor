#' Function to get observations from \strong{openair's} \code{importAURN} 
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
#' @import dplyr
#' 
#' @export
insert_aurn_data <- function(con, site, start, end = NA,
                             verbose = TRUE) {
  
  # Ceiling round
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  # Get look-up tables
  df_processes <- import_processes(con, type = "minimal") %>% 
    select(process, 
           site,
           variable)
  
  # Get data
  if (verbose) message("Downloading AURN data with openair...")
  df <- download_aurn(site, start, end)
  
  # Insert if there is data
  if (nrow(df) > 0) {
    
    # Make longer and join, inner join will only keep those in processes table
    df <- df %>% 
      gather(variable, value, -date, -site, na.rm = TRUE) %>% 
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


#' Function to get observations from the United Kingdom's Automatic Urban and 
#' Rural Network (AURN) with \strong{openair's} \code{importAURN} function.
#' 
#' @param site Site code, for example \code{"yk11"}. 
#' 
#' @param start Start year. 
#' 
#' @param end End year. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
download_aurn <- function(site, start = 1990, end = NA) {
  
  # Current year
  if (is.na(end)) end <- lubridate::year(Sys.Date())
  
  suppressWarnings(
    suppressMessages(
      quiet(
        df <- openair::importAURN(site, year = start:end, verbose = FALSE)
      )
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
