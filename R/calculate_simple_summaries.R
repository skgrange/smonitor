#' Function to create simple monthly and annual means for an \code{smonitor}
#' database. 
#' 
#' The summaries created by \code{calculate_simple_summaries} are generally of
#' use for trend analysis. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param processes A vector of processes. 
#' 
#' @param period Period of aggregation to summarise to. Can be both, or either
#' \code{"month"} or \code{"year"}. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Tibble. 
#' 
#' @author Stuart K. Grange
#'
#' @export
calculate_simple_summaries <- function(con, processes, 
                                       period = c("month", "year"), 
                                       verbose = FALSE) {
  
  # Check inputs
  period <- stringr::str_to_lower(period)
  stopifnot(period %in% c("month", "year"))
  
  # Get observations
  if (verbose) message(threadr::date_message(), "Importing observations...")
  
  df <- import_by_process(
    con, 
    process = processes, 
    site_name = FALSE, 
    date_end = FALSE,
    valid_only = TRUE,
    tz = "UTC"
  ) 
  
  if (nrow(df) != 0) {
    
    # Rename variable
    df <- rename(df, summary_source = summary)
    
    # Calculate the summaries
    df <- purrr::map_dfr(
      period,
      ~calculate_simple_summaries_aggregator(
        df = df, 
        period = .x,
        verbose = verbose
      )
    )
    
  } else {
    
    df <- tibble()
    
  }
  
  # For progress bar
  # .progress = FALSE
  # if (.progress) progress_bar$tick()$print()
  
  return(df)
  
}


calculate_simple_summaries_aggregator <- function(df, period, verbose = TRUE) {
  
  # Use smonitor integers here
  summary_result <- if_else(period == "month", 92L, NA_integer_)
  summary_result <- if_else(period == "year", 102L, summary_result)
  
  # For messages
  period_message <- if_else(period == "month", "monthly", NA_character_)
  period_message <- if_else(period == "year", "annual", period_message)
  
  # Calculate means
  if (verbose) {
    message(threadr::date_message(), "Calculating ", period_message, " summaries...")
  }
  
  quiet(
    df_means <- threadr::aggregate_by_date(
      df,
      interval = period, 
      by = c("site", "variable", "summary_source"),
      summary = "mean"
    )
  )
  
  quiet(
    df_counts <- threadr::aggregate_by_date(
      df,
      interval = period, 
      by = c("site", "variable", "summary_source"),
      summary = "count"
    ) %>% 
      rename(count = value)
  )
  
  # Join aggregations and format return
  df_summaries <- df_means %>% 
    left_join(
      df_counts, 
      by = c("date", "date_end", "site", "variable", "summary_source")
    ) %>% 
    mutate(summary = summary_result) %>% 
    select(date,
           date_end,
           site,
           variable,
           summary_source,
           summary,
           count,
           value) %>% 
    arrange(site,
            summary,
            variable,
            date)
  
  return(df_summaries)
  
}
