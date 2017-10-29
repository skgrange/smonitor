#' Function to calculate trend summaries of observations in an \strong{smonitor}
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @param df Mapping data frame which contains \code{"site"} and 
#' \code{"process"} variables.
#' 
#' @param interval What interval should the trends be; \code{"month"} or 
#' \code{"year"}? 
#' 
#' @param verbose Should the function print what site and processes are being 
#' processed? 
#' 
#' @param progress What type of progress bar should be displayed? 
#' 
#' @author Stuart K.Grange
#' 
#' @export
calculate_trend_summaries <- function(con, df, interval = "month", verbose = TRUE, 
                                      progress = "none") {
  
  # Do by site
  plyr::ddply(
    df, 
    "site", 
    function(x) 
      calculate_trend_summaries_worker(
        con, 
        x, 
        interval = interval, 
        verbose = verbose
      ), 
    .progress = progress
  )
  
}


calculate_trend_summaries_worker <- function(con, df, interval, verbose) {
  
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
    
    import_by_process(
      con, 
      process, 
      site_name = FALSE, 
      date_end = FALSE,
      valid_only = TRUE
    )
    
  }, error = function(e) {
    
    data.frame()
    
  })
  
  if (nrow(df) != 0) {
    
    # Means
    df_mean <- threadr::aggregate_by_date(
      df, 
      interval = interval, 
      by = c("site", "variable", "summary"),
      summary = "mean"
    )
    
    # Counts
    df_count <- threadr::aggregate_by_date(
      df, 
      interval = interval, 
      by = c("site", "variable", "summary"),
      summary = "count", 
      pad = FALSE
    ) %>% 
      rename(count = value)
    
    # Join the summaries together
    df <- df_mean %>% 
      left_join(
        df_count, 
        by = c("date", "date_end", "site", "variable", "summary")
      ) %>% 
      mutate(count = ifelse(is.na(count), 0, count),
             date_insert = as.integer(NA)) %>% 
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
  
  return(df)
  
}
