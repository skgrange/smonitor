#' Function to get UK's AURN data using the same API as \strong{openair}. 
#' 
#' @param site Vector of sites. 
#' 
#' @param year Vector of years. 
#' 
#' @param longer Should the data frame be reshaped to be longer with the use of
#' \strong{tidyr}'s \code{gather} function? 
#' 
#' @param print_query Should the function print the URLs which are being loaded?
#' 
#' @return Data frame. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
get_aurn_data <- function(site, year, longer = FALSE, print_query = FALSE) {
  
  # Build url strings
  site <- stringr::str_to_upper(site)
  
  df_inputs <- expand.grid(
    site = site, 
    year = year,
    stringsAsFactors = FALSE
  )
  
  # Build urls
  url_base <- "https://uk-air.defra.gov.uk/openair/R_data/"
  url <- stringr::str_c(url_base, df_inputs$site, "_", df_inputs$year, ".RData")
  
  # Do
  df <- suppressWarnings(
    data_frame(url = url) %>% 
      rowwise() %>% 
      do(get_aurn_data_worker(.$url, print_query = print_query)) %>% 
      ungroup()
  )
  
  # Immediate name cleaning
  names(df) <- stringr::str_to_lower(names(df))
  names(df) <- ifelse(names(df) == "noxasno2", "nox", names(df))
  
  # Clean site things
  names(df) <- ifelse(names(df) == "site", "site_name", names(df))
  names(df) <- ifelse(names(df) == "code", "site", names(df))
  df$site <- stringr::str_to_lower(df$site)
  
  # Use look-up table to create smonitor names
  df_names <- data.frame(
    variable = names(df),
    stringsAsFactors = FALSE
  )
  
  # Join look-up
  df_names <- left_join(df_names, load_openair_variable_helper(), by = "variable")
  
  # Catch non-matching names
  df_names$variable_smonitor <- ifelse(
    is.na(df_names$variable_smonitor), 
    df_names$variable, 
    df_names$variable_smonitor
  )
  
  # Overwrite names
  names(df) <- df_names$variable_smonitor
  
  # Arrange variables
  df <- threadr::arrange_left(df, c("date", "site", "site_name"))
  
  # Reshape
  if (longer) {
    
    df <- tidyr::gather(
      df, 
      variable, 
      value, 
      -date, -site, -site_name, 
      na.rm = TRUE
    )
    
  }
  
  # Return
  df
  
}


# The worker
get_aurn_data_worker <- function(url, print_query) {
  
  # Messgage
  if(print_query) message(url)
  
  # Build file name
  file_name <- basename(url)
  file_name <- file.path(tempdir(), file_name)
  
  # Download file
  download.file(url, file_name, method = "curl", quiet = TRUE, mode = "wb")
  
  # Load data
  df <- tryCatch({
    
    df <- suppressWarnings(
      load(file_name)
    )
    
    # Assign object
    df <- get(df)
    
    # Return
    df
    
  }, error = function(e) {
    
    # Empty
    data.frame()
    
  })
  
  # Return
  df
  
}
