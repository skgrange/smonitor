#' Function to calculate time-series summaries for data stored in a 
#' \strong{smonitor} database. 
#' 
#' \code{calculate_summaries} will handle aggregation methods, transformation,
#' and updating processes when the \strong{smonitor} tables are complete.
#' 
#' To-do: Optimise to stop many select and insert steps with 
#' \code{aggregate_by_date}. 
#' 
#' \code{calculate_summaries} will correctly aggregate wind direction.
#' 
#' @seealso \code{\link{timeAverage}}, \code{\link{dplyr::summarise}}
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param df_map A mapping table containing \code{"process"} and \code{"summary"}
#' variables. 
#' 
#' @param start Start date for summaries. 
#' 
#' @param end End date for summaries. 
#' 
#' @param insert Should the data be inserted? Default is \code{TRUE}. 
#' 
#' @importFrom magrittr %>%
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
calculate_summaries <- function(con, df_map, start, end, tz = "UTC", 
                                insert = TRUE) {
  
  plyr::a_ply(df_map, 1, function(x) 
    summary_calculator(con, x, start, end, tz = tz, insert))
  
  # No return
  
}
  

# No export
summary_calculator <- function(con, df_map, start, end, insert, tz) {
  
  # Print what is happening
  message(threadr::to_json(df_map))
  
  # Drop tbl_df
  df_map <- threadr::base_df(df_map)
  
  # Get mapping table
  df_look <- import_summaries(con, extra = TRUE)
  
  # Filter mapping table
  df_look <- df_look[df_look$process == df_map$process & 
                       df_look$summary == df_map$summary, ]
  
  # Stop if duplicated mappings
  if (nrow(df_look) > 1) 
    stop("Duplicate process-summary pairs found in `summaries` table.", 
         call. = FALSE)
  
  
  # Different logic for the different aggregation periods
  if (df_look$source == "source" & df_look$period == "hour") {
    
    # Message
    message_querying()
    
    # Get valid observations
    df <- import_by_process(con, df_map$process, start = start, end = end,
                            tz = tz, valid_only = TRUE, site_name = FALSE,
                            date_end = FALSE)
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Alter name for correct wd aggregation
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "value", "wd", names(df))
      
      # Source to hourly
      df_agg <- df %>%
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>%
        dplyr::ungroup() %>%
        threadr::factor_coerce()
      
      # Back to 'value' in aggregations
      if (df_look$variable == "wd")
        names(df_agg) <- ifelse(names(df_agg) == "wd", "value", names(df_agg))
      
      # To-do: catch nas
      if (df_look$aggregation_function == "frequency") {
        
        # Just use minutes for now
        df_agg$value <- df_agg$value / 60
        df_agg$value <- ifelse(is.na(df_agg$value), 0, df_agg$value)
        
      }
      
      # Transform for database
      df_agg <- df_agg %>%
        dplyr::mutate(date_end = date + minutes(59) + seconds(59),
                      date = as.integer(date),
                      date_end = as.integer(date_end),
                      process = as.integer(df_look$process),
                      summary = as.integer(df_look$summary),
                      validity = NA) %>%
        dplyr::select(date,
                      date_end,
                      process,
                      summary,
                      validity,
                      value)
      
    } else {
      
      df_agg <- NULL
      
    }
    
  }
  
  
  if (df_look$source == "hour" & df_look$period == "day") {
    
    # Message
    message_querying()
    
    # Get hourly means
    df <- import_by_process(con, df_map$process, summary = 1, start = start, 
                            end = end, tz = tz, valid_only = TRUE, 
                            site_name = FALSE, date_end = FALSE)
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Alter name for correct wd aggregation
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "value", "wd", names(df))
      
      # Hourly to daily
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        dplyr::ungroup() %>% 
        threadr::factor_coerce() 
      
      # Back to 'value' in aggregations
      if (df_look$variable == "wd")
        names(df_agg) <- ifelse(names(df_agg) == "wd", "value", names(df_agg))

      # Simple data capture
      if (df_look$aggregation_function == "frequency")
        df_agg$value <- ifelse(is.na(df_agg$value), 0, df_agg$value / 24)
      
      # Transform for database
      df_agg <- df_agg %>% 
        dplyr::mutate(date_end = date + hours(23) + minutes(59) + seconds(59),
                      date = as.integer(date),
                      date_end = as.integer(date_end),
                      process = as.integer(df_look$process),
                      summary = as.integer(df_look$summary),
                      validity = NA) %>% 
        dplyr::select(date,
                      date_end,
                      process,
                      summary,
                      validity,
                      value)
      
    } else {
      
      df_agg <- NULL
      
    }
    
  }
  
  
  if (df_look$source == "day" & df_look$period == "month") {
    
    # Message
    message_querying()
    
    # Get daily means
    df <- import_by_process(con, df_map$process, summary = 20, start = start, 
                            end = end, tz = tz, valid_only = TRUE, site_name = FALSE, 
                            date_end = FALSE)
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Alter name for correct wd aggregation
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "value", "wd", names(df))
      
      # Daily to monthly
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        dplyr::ungroup() %>% 
        threadr::factor_coerce() 
      
      # Back to 'value' in aggregations
      if (df_look$variable == "wd")
        names(df_agg) <- ifelse(names(df_agg) == "wd", "value", names(df_agg))
      
      # Transform for database
      df_agg <- df_agg %>% 
        dplyr::mutate(date_end = ceiling_date(date + 1, "month") - 1,
                      date = as.integer(date),
                      date_end = as.integer(date_end),
                      process = as.integer(df_look$process),
                      summary = as.integer(df_look$summary),
                      validity = NA) %>% 
        dplyr::select(date,
                      date_end,
                      process,
                      summary,
                      validity,
                      value)
      
    } else {
      
      df_agg <- NULL
      
    }
    
  }
  
  
  if (df_look$source == "hour" & df_look$period == "year") {
    
    # Message
    message_querying()
    
    # Get hourly means
    df <- import_by_process(con, df_map$process, summary = 1, start = start, 
                            end = end, tz = tz, valid_only = TRUE, site_name = FALSE, 
                            date_end = FALSE)

    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Alter name for correct wd aggregation
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "value", "wd", names(df))
      
      # Hourly to annual
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        dplyr::ungroup() %>% 
        threadr::factor_coerce()
      
      # Back to 'value' in aggregations
      if (df_look$variable == "wd")
        names(df_agg) <- ifelse(names(df_agg) == "wd", "value", names(df_agg))
      
      # To-do: catch nas
      if (df_look$aggregation_function == "frequency") {
        
        df_agg$leap_year <- lubridate::leap_year(df_agg$date)
        df_agg$value <- ifelse(df_agg$leap_year, df_agg$value / 8784, 
                               df_agg$value / 8760)
        
      }
      
      # Transform for database
      df_agg <- df_agg %>% 
        dplyr::mutate(date_end = ceiling_date(date + 1, "year") - 1,
                      date = as.integer(date),
                      date_end = as.integer(date_end),
                      process = as.integer(df_look$process),
                      summary = as.integer(df_look$summary),
                      validity = NA) %>% 
        dplyr::select(date,
                      date_end,
                      process,
                      summary,
                      validity,
                      value)
      
    } else {
      
      df_agg <- NULL
      
    }
    
  }
  
  
  if (df_look$source == "day" & df_look$period == "year") {
    
    # Message
    message_querying()
    
    # Get daily means
    df <- import_by_process(con, df_map$process, summary = 20, start = start, 
                            end = end, tz = tz, valid_only = TRUE, site_name = FALSE,
                            date_end = FALSE)
    
    df$date_end <- NULL
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Alter name for correct wd aggregation
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "value", "wd", names(df))
      
      # Daily to annual
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        dplyr::ungroup() %>% 
        threadr::factor_coerce() 
      
      # Back to 'value' in aggregations
      if (df_look$variable == "wd")
        names(df_agg) <- ifelse(names(df_agg) == "wd", "value", names(df_agg))
      
      if (df_look$aggregation_function == "frequency") {
        
        # Leap year days
        df_agg$leap_year <- lubridate::leap_year(df_agg$date)
        df_agg$value <- ifelse(df_agg$leap_year, df_agg$value / 366, df_agg$value / 365)
        
        # Drop
        df_agg$leap_year <- NULL
        
      }
      
      # Transform for database
      df_agg <- df_agg %>% 
        dplyr::mutate(date_end = ceiling_date(date + 1, "year") - 1,
                      date = as.integer(date),
                      date_end = as.integer(date_end),
                      process = as.integer(df_look$process),
                      summary = as.integer(df_look$summary),
                      validity = NA) %>% 
        dplyr::select(date,
                      date_end,
                      process,
                      summary,
                      validity,
                      value)
      
    } else {
      
      df_agg <- NULL
      
    }
    
  }
  
  
  # What to do with the summary? 
  if (insert) {
    
    if (!is.null(df_agg)) {
      
      # Delete old observations
      message("Deleting old observations...")
      delete_observations(con, df_agg, match = "between", progress = "none")
      
      message("Inserting new observations...")
      insert_observations(con, df_agg)
      
    } else {
      
      # Also add variable here
      df_agg$date_insert <- sys_unix_time(integer = TRUE)
      message("No data inserted...")
      
    }
    
  } else {
    
    # Return
    df_agg
    
  }
  
}


# No export needed
message_summary <- function() message("Aggregating data...")
message_querying <- function() message("Querying database...")
