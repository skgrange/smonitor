#' Function to import monthly summaries from an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param site Sites to import. 
#' 
#' @param variable Variables to import. 
#' 
#' @param period Periods to import
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
#' @author Stuart K. Grange
#' 
#' @return Data frame.
#' 
#' @export
import_monthly_summaries <- function(con, site = NA, variable = NA, period = NA,
                                     date_insert = FALSE, spread = FALSE,
                                     tz = "UTC") {
  
  # Switch period
  period <- ifelse(period %in% c("hour", "hourly"), 1, period)
  period <- ifelse(period %in% c("day", "daily"), 20, period)
  
  # Clean
  site <- stringr::str_trim(site)
  site <- stringr::str_to_lower(site)
  
  sql <- "SELECT date_insert, 
          date, 
          date_end,
          summary,
          site,
          variable,
          count,
          value 
          FROM observations_monthly
          ORDER BY site,
          summary,
          variable,
          date"
  
  # Add site where clause
  if (!is.na(site[1])) {
    
    site <- stringr::str_c("'", site, "'")
    site <- stringr::str_c(site, collapse = ",")
    sql_site_where <- stringr::str_c(" WHERE site IN (", site, ")")
    
    # Add to statement
    sql <- stringr::str_replace(sql, "observations_monthly", 
      stringr::str_c("observations_monthly", sql_site_where))
    
  }
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query database
  df <- databaser::db_get(con, sql)
  
  # Do variable filtering here, to-do add to sql
  if (!is.na(variable[1])) 
    df <- df[df$variable %in% variable, ]
  
  if (!is.na(period[1]))
    df <- df[df$summary %in% period, ]
  
  # Clean-up a bit
  df <- df %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz),
           date_end = threadr::parse_unix_time(date_end, tz = tz))
  
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
    df <- df %>% 
      tidyr::spread(variable, value)
    
  }
  
  # Return
  df
  
}
