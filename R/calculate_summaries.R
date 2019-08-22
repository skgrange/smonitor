#' Function to calculate time series summaries for data stored in a 
#' \strong{smonitor} database. 
#' 
#' \code{calculate_summaries} will handle aggregation methods, transformation, 
#' and updating processes when the \strong{smonitor} tables are complete.
#' 
#' \code{calculate_summaries} will correctly aggregate wind direction.
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param df A mapping table containing \code{"process"}, \code{"source_summary"},
#' \code{"summary"}, \code{"summary_name"}, \code{"period"}, 
#' \code{"aggregation_function"}, \code{"validity_threshold"}, and 
#' \code{"source_name_summary"} variables. 
#' 
#' @param start Start date for summaries. 
#' 
#' @param end End date for summaries. 
#' 
#' @param tz Time zone in use. 
#' 
#' @param insert Should the data be inserted? This is a useful option to have
#' to allow for a dry run of an insert. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Invisible database connection.
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
#' calculate_summaries(con, data_look_up, start = 2014, start = 2014)
#' 
#' }
#' 
#' @export
calculate_summaries <- function(con, df, start = NA, end = NA, tz = "UTC", 
                                insert = TRUE, verbose = FALSE) {
  
  # Check input
  variables_needed <- c(
    "process", "source_summary", "summary", "summary_name", "period", 
    "aggregation_function", "validity_threshold", "source_name_summary"
  )
  
  if (!all(variables_needed %in% names(df))) {
    stop("Mandatory variables not present in mapping data frame.", call. = FALSE)
  }
  
  # Do
  df %>% 
    split(list(.$summary, .$aggregation_function, .$validity_threshold)) %>% 
    purrr::discard(~nrow(.) == 0) %>% 
    purrr::walk(
      ~calculate_summaries_worker(
        con, 
        df = .x,
        start = start,
        end = end,
        tz = tz, 
        verbose = verbose,
        insert = insert
      )
    )
  
  return(invisible(con))
  
}


calculate_summaries_worker <- function(con, df, start, end, tz, verbose, 
                                       insert) {
  # Get keys
  processes <- sort(df$process)
  
  # Single length vectors
  source_summary <- df$source_summary[1]
  
  # Aggregate by date uses friendly identifier
  period <- df$period[1]
  summary <- df$summary[1]
  
  # Only for message
  summary_name <- df$summary_name[1]
  source_name_summary <- df$source_name_summary[1]
  
  # Others
  aggregation_function <- df$aggregation_function[1]
  validity_threshold <- df$validity_threshold[1]
  validity_threshold <- if_else(is.na(validity_threshold), 0, validity_threshold)
  
  # Catch
  if (validity_threshold > 1) validity_threshold <- validity_threshold / 100
  
  if (verbose) {
    
    # Build and print a message
    stringr::str_c(
      threadr::date_message(),
      "Calculating summaries for ",
      "`processes` c(", stringr::str_c(processes, collapse = ","), ")",
      " from `", source_name_summary, "`",
      " to `", summary_name, "`",
      " with a `", validity_threshold, "` validity threshold..."
    ) %>% 
      message()
    
    # " and `", aggregation_function, "`"
    
  }
  
  # Import source data
  if (verbose) message(threadr::date_message(), "Importing observations...")
  
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
      pad_unit <- if_else(grepl("day|daily", source_name_summary), "day", NA_character_)
      pad_unit <- if_else(grepl("hour|hourly", source_name_summary), "hour", pad_unit)
      pad_unit <- if_else(grepl("month|monthly", source_name_summary), "month", pad_unit)
      
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
    if (verbose) message(threadr::date_message(), "Doing the aggregation...")
    
    df_agg <- threadr::aggregate_by_date(
      df, 
      interval = period, 
      by = "process",
      summary = aggregation_function,
      threshold = validity_threshold
    ) %>% 
      mutate(summary = summary,
             validity = NA)
    
    # Only insert if stated
    if (insert) {
      
      # Delete old observations
      delete_observations(con, df_agg, match = "between", verbose = verbose)
      
      # Insert aggregated observations
      insert_observations(con, df_agg, verbose = verbose)
      
    } else {
      
      if (verbose) 
        message(threadr::date_message(), "No data deleted or inserted into database...")
      
    }
    
  } else {
    
    if (verbose) message(threadr::date_message(), "No observations imported...")
    
  }
  
  # No return
  
}


#' @rdname calculate_summaries
#' 
#' @export
calculate_summaries_efficently <- calculate_summaries
