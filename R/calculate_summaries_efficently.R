#' Function to calculate time-series summaries for data stored in a 
#' \strong{smonitor} database. 
#' 
#' \code{calculate_summaries_efficently} will handle aggregation methods, 
#' transformation, and updating processes when the \strong{smonitor} tables are
#' complete.
#' 
#' \code{calculate_summaries_efficently} will correctly aggregate wind direction.
#' 
#' @seealso 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param df_map A mapping table containing \code{"process"}, \code{"summary"},
#' \code{"aggregation_function"}, and \code{"validity_threshold"} variables. 
#' 
#' @param start Start date for summaries. 
#' 
#' @param end End date for summaries. 
#' 
#' @param tz Time zone in use. 
#' 
#' @param insert Should the data be inserted? Default is \code{TRUE}. 
#' 
#' @param verbose Should the function be verbose? 
#' 
#' @param progress Type of progress bar to display. 
#' 
#' @return Invisible. 
#' 
#' @seealso \code{\link{import_summaries}}, \code{\link{timeAverage}}, 
#' \code{\link{summarise}}, \code{\link{time_pad}}, 
#' \code{\link{aggregate_by_date}}
#' 
#' @examples 
#' \dontrun{
#'
#' # Calculate all summaries in data_look_up for 2014 and insert them into the 
#' # database
#' calculate_summaries_efficently(con, data_look_up, start = 2014, start = 2014)
#' 
#' }
#' 
#' @export
calculate_summaries_efficently <- function(con, df_map, start = NA, end = NA, 
                                           tz = "UTC", insert = TRUE, 
                                           verbose = FALSE, progress = "none") {
  
  # Check input
  variables_needed <- c("summary", "aggregation_function", "validity_threshold")
  
  if (!all(variables_needed %in% names(df_map)))
    stop("Mandatory variables not present in mapping data frame", call. = FALSE)
  
  plyr::d_ply(
    df_map, 
    c("summary", "aggregation_function", "validity_threshold"),
    function(x) calculate_summaries_efficently_worker(
      con, 
      x, 
      start, 
      end, 
      tz, 
      verbose, 
      insert),
    .progress = progress
  )
  
  # No return
  
}


calculate_summaries_efficently_worker <- function(con, df_map, start, end, tz, 
                                                  verbose, insert) {
  # Get keys
  processes <- sort(df_map$process)
  
  # Single length vectors
  source_summary <- df_map$source_summary[1]
  source_summary_name <- df_map$source_name_summary[1]
  
  # Aggregate by date uses friendly identifier
  period <- df_map$period[1]
  summary <- df_map$summary[1]
  
  # Others
  aggregation_function <- df_map$aggregation_function[1]
  validity_threshold <- df_map$validity_threshold[1]
  validity_threshold <- ifelse(is.na(validity_threshold), 0, validity_threshold)
  
  # Catch
  if (validity_threshold > 1) validity_threshold <- validity_threshold / 100
  
  if (verbose) {
    
    message(
      threadr::to_json(
        list(
          source_summary_name = source_summary_name,
          summary_name = df_map$summary_name[1],
          aggregation_function = aggregation_function,
          validity_threshold = validity_threshold,
          processes = processes
        )
      )
    )
    
  }
  
  # Import source data
  df <- import_by_process(
    con, 
    process = processes, 
    summary = source_summary,
    start = start, 
    end = end, 
    date_end = FALSE, 
    site_name = FALSE, 
    date_insert = FALSE, 
    warn = FALSE
  )
  
  # Only continue if observations are present
  if (nrow(df) != 0) {
    
    # Need yearly padded time-series here
    if (aggregation_function == "data_capture") {
      
      # More are possible
      pad_unit <- ifelse(grepl("day|daily", source_summary_name), "day", NA)
      pad_unit <- ifelse(grepl("hour|hourly", source_summary_name), "hour", pad_unit)
      pad_unit <- ifelse(grepl("month|monthly", source_summary_name), "month", pad_unit)
      
      # Pad
      df <- threadr::time_pad(
        df, 
        interval = pad_unit, 
        by = "process", 
        round = "year", 
        warn = FALSE, 
        full = TRUE
      )
      
    }
    
    # Aggregate/summarise data with mapping table
    df_agg <- threadr::aggregate_by_date(
      df, 
      interval = period, 
      by = "process",
      summary = aggregation_function,
      threshold = validity_threshold
    ) %>% 
      mutate(summary = summary,
             validity = NA)
    
    # Delete old observations
    delete_observations(con, df_agg, match = "between", progress = "none")
    
    # Insert aggregated observations
    insert_observations(con, df_agg)
    
  }
  
  # House keeping
  gc()
  
  # No return
  
}
