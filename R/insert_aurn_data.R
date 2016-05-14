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
  
  # Get look-up
  df_processes <- import_processes(con)
  
  # Filter and select
  df_processes <- df_processes[df_processes$site %in% site, 
                               c("process", "site", "variable")]
  
  # Get observations
  df <- download_aurn(site, start, end)
  
  # Make longer and join, inner join will only keep those in processes table
  df <- df %>% 
    gather(variable, value, -date, -site, na.rm = TRUE) %>% 
    mutate(site = str_to_lower(site),
           date_end = date + 3599,
           date = as.integer(date),
           date_end = as.integer(date_end),
           summary = 1L,
           validity = NA) %>% 
    inner_join(df_processes, c("site", "variable"))
  
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
#' @author Stuart K. Grange
#' 
#' @param site Site code.
#' @param start Start year. 
#' @param end End year. 
#' 
#' @param site Site code. 
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
