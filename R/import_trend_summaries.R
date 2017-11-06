#' Function to import trend summaries from an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param site Sites to import. 
#' 
#' @param variable Variables to import. 
#' 
#' @param aggregation Aggregation to import. 
#' 
#' @param date_insert Should the \code{date_insert} variable be returned? 
#' 
#' @param spread Should the return be reshaped to have variables as column 
#' names? The \code{date_insert} and \code{count} variables are dropped so this
#' can occur. 
#' 
#' @param tz What time-zone should the dates be returned as? Default is 
#' \code{"UTC"}. 
#' 
#' @param print_query Should the SQL query statement be printed? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame.
#' 
#' @export
import_trend_summaries <- function(con, site = NA, variable = NA, 
                                   aggregation = "monthly_mean",
                                   date_insert = FALSE, spread = FALSE,
                                   tz = "UTC", print_query = FALSE) {
  
  # Clean
  site <- stringr::str_trim(site)
  site <- stringr::str_to_lower(site)
  aggregation <- stringr::str_trim(aggregation)
  aggregation <- stringr::str_to_lower(aggregation)
  
  # Switch aggregation
  aggregation <- ifelse(
    aggregation %in% c("monthly_mean", "month_mean"), 
    92, 
    aggregation
  )
  
  aggregation <- ifelse(
    aggregation %in% c("year_mean", "annual_mean"), 102, 
    aggregation
  )
  
  # Build
  sql <- stringr::str_c(
    "SELECT date_insert, 
    date, 
    date_end,
    summary,
    aggregation,
    site,
    variable,
    count,
    value 
    FROM observations_trend_summaries
    WHERE aggregation = ", aggregation, "
    ORDER BY site,
    summary,
    variable,
    date"
  )
  
  # Add site where clause
  if (!is.na(site[1])) {
    
    site <- stringr::str_c("'", site, "'")
    site <- stringr::str_c(site, collapse = ",")
    
    sql_site_and <- stringr::str_c(" AND site IN (", site, ")")
    
    # Add to statement
    sql <- stringr::str_replace(
      sql,
      "ORDER BY", 
      stringr::str_c(sql_site_and, " ORDER BY ")
    )
    
  }
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Print
  if (print_query) message(sql)
  
  # Query database
  df <- databaser::db_get(con, sql)
  
  if (nrow(df) == 0) {
    
    # Empty data frame
    return(data.frame())
    
  } else {
    
    # Do variable filtering here, to-do add to sql
    if (!is.na(variable[1])) 
      df <- df[df$variable %in% variable, ]
    
    # Clean-up a bit
    df$date <- threadr::parse_unix_time(df$date, tz = tz)
    df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
    
    if (date_insert) {
      
      df$date_insert <- threadr::parse_unix_time(df$date_insert, tz = tz)
      
    } else {
      
      df$date_insert <- NULL
      
    }
    
    if (spread) {
      
      # Drop
      df$date_end <- NULL
      df$count <- NULL
      
      # Make wider
      df <- tidyr::spread(df, variable, value)
      
    }
    
    return(df)
    
  }
  
}
