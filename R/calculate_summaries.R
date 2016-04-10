#' Function to calculate various time-series summaries.
#' 
#' \code{calculate_summaries} will correctly process wind speed and direction
#' with the help of \strong{openair}. 
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
#' @param insert Should the data be inserted? 
#' 
#' @import dplyr
#' @import threadr
#' 
#' @export
calculate_summaries <- function(con, df_map, start, end, insert = FALSE) {
  
  # Print what is happening
  message(jsonlite::toJSON(df_map, pretty = TRUE))
  
  # Demote
  df_map <- threadr::base_df(df_map)
  
  # Get mapping table
  df_look <- import_summaries(con, extra = TRUE)
  
  # Filter mapping table
  df_look <- df_look[df_look$process == df_map$process & 
                       df_look$summary == df_map$summary, ]
  
  
  # Different logic for the different aggregation periods
  if (df_look$source == "source" & df_look$period == "hour") {
    
    # Get observations
    df <- import_source(con, df_map$process, start = start, end = end,
                        valid = TRUE)
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Alter name for correct wd aggregations
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "value", "wd", names(df))
      
      # Source to hourly
      df_agg <- df %>%
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>%
        ungroup() %>%
        factor_coerce()
      
      # Back to value
      if (df_look$variable == "wd")
        names(df) <- ifelse(names(df) == "wd", "value", names(df))
      
      # To-do: catch nas
      if (df_look$aggregation_function == "frequency") {
        
        # Just use minutes for now
        df_agg$value <- df_agg$value / 60
        df_agg$value <- ifelse(is.na(df_agg$value), 0, df_agg$value)
        
      }
      
      # Transform for database
      df_agg <- df_agg %>%
        mutate(date_end = date + minutes(59) + seconds(59),
               date = as.integer(date),
               date_end = as.integer(date_end),
               process = as.integer(df_look$process),
               summary = as.integer(df_look$summary),
               validity = NA) %>%
        select(date,
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
    
    # Load
    df <- import_hourly_means(con, df_map$process, start = start, end = end)
    df$date_end <- NULL
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Hourly to daily
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        ungroup() %>% 
        factor_coerce() %>% 
        mutate(date_end = date + hours(23) + minutes(59) + seconds(59),
               date = as.integer(date),
               date_end = as.integer(date_end),
               process = as.integer(df_look$process),
               summary = as.integer(df_look$summary),
               validity = NA) %>% 
        select(date,
               date_end,
               process,
               summary,
               validity,
               value)
      
      # Simple
      if (df_look$aggregation_function == "frequency")
        df_agg$value <- ifelse(is.na(df_agg$value), 0, df_agg$value / 24)
      
    } else {
      
      df_agg <- NULL
      
    }
    
  }
  
  
  if (df_look$source == "day" & df_look$period == "month") {
    
    # Load
    df <- import_daily_means(con, df_map$process, start = start, end = end)
    df$date_end <- NULL
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Daily to monthly
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        ungroup() %>% 
        factor_coerce() %>% 
        mutate(date_end = ceiling_date(date + 1, "month") - 1,
               date = as.integer(date),
               date_end = as.integer(date_end),
               process = as.integer(df_look$process),
               summary = as.integer(df_look$summary),
               validity = NA) %>% 
        select(date,
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
    
    # Load
    df <- import_hourly_means(con, df_map$process, start = start, end = end)
    df$date_end <- NULL
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Hourly to annual
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        ungroup() %>% 
        factor_coerce() 
      
      # To-do: catch nas
      if (df_look$aggregation_function == "frequency") {
        
        df_agg$leap_year <- lubridate::leap_year(df_agg$date)
        df_agg$value <- ifelse(df_agg$leap_year, df_agg$value / 8784, 
                               df_agg$value / 8760)
        
      }
      
      df_agg <- df_agg %>% 
        mutate(date_end = ceiling_date(date + 1, "year") - 1,
               date = as.integer(date),
               date_end = as.integer(date_end),
               process = as.integer(df_look$process),
               summary = as.integer(df_look$summary),
               validity = NA) %>% 
        select(date,
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
    
    # Load
    df <- import_daily_means(con, df_map$process, start = start, end = end)
    df$date_end <- NULL
    
    # Only if data
    if (nrow(df) > 0) {
      
      # Message
      message_summary()
      
      # Daily to annual
      df_agg <- df %>% 
        openair::timeAverage(avg.time = df_look$period,
                             data.thresh = df_look$validity_threshold,
                             statistic = df_look$aggregation_function) %>% 
        ungroup() %>% 
        factor_coerce() 
      
      if (df_look$aggregation_function == "frequency") {
        
        # Leap year days
        df_agg$leap_year <- lubridate::leap_year(df_agg$date)
        df_agg$value <- ifelse(df_agg$leap_year, df_agg$value / 366, df_agg$value / 365)
        
        # Drop
        df_agg$leap_year <- NULL
        
      }
      
      # Transform
      df_agg <- df_agg %>% 
        mutate(date_end = ceiling_date(date + 1, "year") - 1,
               date = as.integer(date),
               date_end = as.integer(date_end),
               process = as.integer(df_look$process),
               summary = as.integer(df_look$summary),
               validity = NA) %>% 
        select(date,
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
      delete_observations(con, df_agg, match = "between")
      
      message("Inserting new observations...")
      threadr::db_insert(con, "observations", df_agg)
      
    } else {
      
      message("No data inserted...")
      
    }

  } else {
    
    # Return
    df_agg
    
  }
  
}


# No exported needed
message_summary <- function() message("Aggregating data...")
