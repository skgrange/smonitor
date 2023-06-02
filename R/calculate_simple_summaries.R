#' Function to create simple monthly and annual means for an \strong{smonitor}
#' database. 
#' 
#' The summaries created by \code{calculate_simple_summaries} are generally of
#' use for trend analysis. 
#' 
#' @author Stuart K. Grange.
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param processes A vector of processes. 
#' 
#' @param start What is the start date of observations to be summarised?
#' 
#' @param end What is the end date of observations to be summarised?
#' 
#' @param period Period of aggregation to summarise to. Can be both, or either
#' \code{"month"} or \code{"year"}. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Tibble. 
#'
#' @export
calculate_simple_summaries <- function(con, processes, start = NA, end = NA,
                                       period = c("month", "year"), 
                                       verbose = FALSE) {
  
  # Check inputs
  period <- stringr::str_to_lower(period)
  stopifnot(period %in% c("month", "year"))
  
  # For query
  if (is.na(start)) start <- 1969
  
  # Get observations
  if (verbose) {
    message(
      threadr::date_message(), "Summarising `", length(processes), "` processes..."
    )
  }
  
  df <- import_by_process(
    con, 
    process = processes, 
    start = start,
    end = end,
    site_name = FALSE, 
    date_end = FALSE,
    valid_only = TRUE,
    tz = "UTC",
    warn = FALSE
  ) 
  
  if (nrow(df) != 0) {
    
    # Rename variable
    df <- rename(df, summary_source = summary)
    
    # Calculate the summaries
    df <- purrr::map_dfr(
      period,
      ~calculate_simple_summaries_worker(
        df = df, 
        period = .x,
        verbose = verbose
      )
    ) %>% 
      arrange(site,
              summary,
              variable,
              date)
    
  } else {
    df <- tibble()
  }
  
  return(df)
  
}


calculate_simple_summaries_worker <- function(df, period, verbose) {
  
  # Use smonitor integers here
  summary_result <- if_else(period == "month", 92L, NA_integer_)
  summary_result <- if_else(period == "year", 102L, summary_result)
  
  # Two calls here
  df_means <- threadr::aggregate_by_date(
    df,
    interval = period, 
    by = c("site", "variable", "summary_source"),
    summary = "mean"
  )
  
  df_counts <- threadr::aggregate_by_date(
    df,
    interval = period, 
    by = c("site", "variable", "summary_source"),
    summary = "count"
  ) %>% 
    rename(count = value)
  
  # Join aggregations and format return
  df_summaries <- df_means %>% 
    left_join(
      df_counts, 
      by = c("date", "date_end", "site", "variable", "summary_source")
    ) %>% 
    mutate(summary = !!summary_result) %>% 
    select(date,
           date_end,
           site,
           variable,
           summary_source,
           summary,
           count,
           value)
  
  return(df_summaries)
  
}
