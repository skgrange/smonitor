#' Function to calculate monthly means of observations in an \strong{smonitor}
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @param df Mapping data frame which contains \code{"site"} and 
#' \code{"process"} variables.
#' 
#' @param verbose Should the function print what site and processes are being 
#' processed? 
#' 
#' @param progress What type of progress bar should be displayed? 
#' 
#' @author Stuart K.Grange
#' 
#' @import dplyr
#' 
#' @export
calculate_monthly_summaries <- function(con, df, verbose = TRUE, 
                                        progress = "none") {
  
  # Do by site
  plyr::ddply(df, "site", function(x) 
    calculate_monthly_summaries_worker(con, x, verbose = verbose), 
    .progress = progress)
  
}


calculate_monthly_summaries_worker <- function(con, df, verbose) {
  
  site <- df$site[1]
  process <- df$process
  
  if (verbose) {
    
    df_message <- data.frame(
      site = site,
      process = stringr::str_c(process, collapse = ", "),
      stringsAsFactors = FALSE
    )
    
    message(threadr::to_json(df_message))
    
  }
  
  # Get observations
  df <- tryCatch({
    
    import_any(con, process, site_name = FALSE, date_end = FALSE, 
               valid_only = TRUE)
    
  }, error = function(e) {
    
    data.frame()
    
  })
  
  if (nrow(df) != 0) {
    
    # Summarise
    df <- df %>% 
      mutate(year = lubridate::year(date), 
             month = lubridate::month(date),
             month = stringr::str_pad(month, width = 2, pad = "0")) %>% 
      group_by(year,
               month,
               site,
               summary,
               variable) %>% 
      summarise(count = sum(!is.na(value)), 
                value = mean(value, na.rm = TRUE)) %>% 
      ungroup() %>% 
      mutate(date = stringr::str_c(year, month, "01"),
             date = lubridate::ymd(date, tz = "UTC"),
             date_end = lubridate::ceiling_date(date, unit = "month",
                                                change_on_boundary = TRUE),
             date_end = date_end - 1,
             date_insert = as.integer(NA)) %>% 
      select(-year,
             -month) %>%
      select(date_insert,
             date,
             date_end,
             summary,
             everything()) %>% 
      arrange(site,
              summary,
              variable,
              date)
    
  } else {
    
    # In case a data frame is returned with only headers
    data.frame()
    
  }
  
  # Return
  df
  
}
