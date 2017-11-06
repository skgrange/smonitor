#' Function to calculate trend summaries of observations in an \strong{smonitor}
#' database. 
#' 
#' @param con Database connection. 
#' 
#' @param df Mapping data frame which contains \code{"site"} and 
#' \code{"process"} variables.
#' 
#' @param interval What interval should the trends be; \code{"month"} and 
#' \code{"year"} are supported. 
#' 
#' @param verbose Should the function print what site and processes are being 
#' processed? 
#' 
#' @param progress What type of progress bar should be displayed? 
#' 
#' @author Stuart K.Grange
#' 
#' @return Invisible.
#' 
#' @export
calculate_trend_summaries <- function(con, df, interval = c("month", "year"), 
                                      verbose = TRUE, progress = "none") {
  
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
  
  # Get what is used
  site <- df$site[1]
  process <- df$process
  
  if (verbose) {
    
    list_message <- list(
      site = site,
      process = process
    )
    
    message(threadr::to_json(list_message))
    
  }
  
  # Get observations
  df <- import_by_process(
    con, 
    process, 
    site_name = FALSE, 
    date_end = FALSE,
    valid_only = TRUE,
    tz = "UTC"
  )
  
  # Calculate the aggregations for trend anaysis
  df <- purrr::map_dfr(interval, calculate_trend_aggregations, df = df)
  
  return(df)
  
}


calculate_trend_aggregations <- function(df, interval) {
  
  if (nrow(df) != 0) {
    
    # smonitor integers
    aggregation <- ifelse(interval == "month", 92L, NA)
    aggregation <- ifelse(interval == "year", 102L, aggregation)
    
    # Means
    df_mean <- threadr::aggregate_by_date(
      df, 
      interval = interval, 
      by = c("site", "variable", "summary"),
      summary = "mean",
      pad = TRUE
    )
    
    # Counts, no need to pad
    df_count <- threadr::aggregate_by_date(
      df, 
      interval = interval, 
      by = c("site", "variable", "summary"),
      summary = "count", 
      pad = FALSE
    ) %>% 
      rename(count = value)
    
    # Join the summaries together and format the table
    df <- df_mean %>% 
      left_join(
        df_count, 
        by = c("date", "date_end", "site", "variable", "summary")
      ) %>% 
      mutate(count = ifelse(is.na(count), 0, count),
             date_insert = as.numeric(NA),
             aggregation = aggregation) %>% 
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
    df <- data.frame()
    
  }
  
  return(df)
  
}
