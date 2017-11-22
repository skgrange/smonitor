#' Function to import data from an \strong{smonitor} database based on sites. 
#' 
#' \code{import_by_site} is considered the primary user-focused importing 
#' function while \code{import_by_process} is the primary lower-level function. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param site A vector of sites. 
#' 
#' @param variable An optional variable vector. If not used, all variables will
#' be returned.  
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param period Averaging period. Default is \code{"hour"}. \code{period} can
#' also take the value \code{NA} which will return all periods, but \code{pad} 
#' argument will be ignored.
#' 
#' @param include_sums Should processes with the appropriate \code{period} which 
#' have been summarised/aggregated with a sum function also be returned? This is
#' useful for variables such as rainfall, snow, and precipitation where normally
#' the aggregations do not take the form of the mean. Default is \code{TRUE}. 
#' 
#' @param valid_only Should only valid data be returned? Default is \code{TRUE}. 
#' 
#' @param pad Should the time-series be padded to ensure all dates in the 
#' observation period are present? Default is \code{TRUE}. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param spread Should the data frame take the wider format resulting from
#' spreading the data? Default is \code{FALSE}. 
#' 
#' @param date_end Should the return include the \code{date_end} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param date_insert Should the return include the \code{date_insert} variable? 
#' Default is \code{FALSE}. 
#' 
#' @param site_name Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param unit Should the processes' units be included in the return? 
#' 
#' @param print_query Should the SQL query string be printed? 
#' 
#' @seealso \code{\link{import_by_process}}
#' 
#' @author Stuart K. Grange
#' 
#' @examples 
#' \dontrun{
#' 
#' # Get hourly means, the default
#' data_site_hour <- import_by_site(con, site = "site_005", start = 2010)
#' 
#' # Get hourly means and spread the data, useful for some analyses
#' data_site_hour_spread <- import_by_site(
#'   con, 
#'   site = "site_005", 
#'   start = 2010,
#'   spread = TRUE
#' )
#' 
#' # Get daily means for two sites
#' data_site_hour <- import_by_site(
#'   con, 
#'   site = c("site_005", "site_100"), 
#'   period = "day", 
#'   start = 2010,
#'   spread = TRUE
#' )
#' 
#' }
#' 
#' @export
import_by_site <- function(con, site = NA, variable = NA, start = 1970, end = NA, 
                           period = "hour", include_sums = TRUE, 
                           valid_only = TRUE, pad = TRUE, tz = "UTC", 
                           spread = FALSE, date_end = TRUE, date_insert = FALSE, 
                           site_name = TRUE, unit = TRUE, print_query = FALSE) {
  
  # Check
  if (is.na(site[1]) || site == "") 
    stop("The `site` argument must be used...", call. = FALSE)
  
  # Parse arguments
  site <- stringr::str_trim(site)
  site <- threadr::str_sql_quote(site)
  
  variable <- stringr::str_trim(variable)
  variable <- threadr::str_sql_quote(variable)
  
  period <- stringr::str_trim(period)
  period <- stringr::str_replace_all(period, " ", "_")
  
  # Check period
  if (!length(period) == 1) 
    stop("Only one `period` can be specified...", call. = FALSE)
  
  if (!period %in% valid_periods) {
    
    stop(
      stringr::str_c(
        "Invalid `period`. Valid options are: ", 
        stringr::str_c(valid_periods, collapse = ", "),
        "..."
      ), 
      call. = FALSE
    )
    
  }
  
  # Switch period to integer, the summary integer used in smonitor
  summary <- ifelse(period == "source", 0, period)
  
  summary <- ifelse(period %in% c("fifteen_minute", "15_minute", "15_min"), 15, summary)
  
  summary <- ifelse(period == "hour", 1, summary)
  
  summary <- ifelse(period == "day", 20, summary)
  
  summary <- ifelse(period == "month", 80, summary)
  
  # Could be another integer depending on source summary, used daily source here
  summary <- ifelse(period %in% c("annual", "year"), 40, summary)
  
  # Time padder requires a string for sequence creation
  interval_pad <- ifelse(summary == 15, "15 min", period)
  
  # Usually for rainfall, precip, and snow
  if (include_sums) {
    
    # ifelse not working here?
    if (summary[1] == 1) summary <- c(summary, 5)
    
    # if (summary[1] == 15) summary <- c(summary, 5)
    
    if (summary[1] == 20) summary <- c(summary, 24)
    
    if (summary[1] == 80) summary <- c(summary, 84)
    
    # Different here because daily sums are not needed to get an annual sum
    if (summary[1] == 40) summary <- c(summary, 64)
    
  }
  
  # Does not make sense to pad data in these situations
  if (period == "source" | is.na(period)) pad <- FALSE
  
  # Get process keys
  # Build sql, only really need process here
  sql_processes <- stringr::str_c(
    "SELECT process,
     site,
     variable
     FROM processes
     WHERE site IN (", site, ")"
  )
  
  # Add variable as where clause too
  if (!is.na(variable[1])) {
    
    sql_processes <- stringr::str_c(
      sql_processes, 
      " AND variable IN (", variable, ")"
    )
    
  }
  
  # Clean statement
  sql_processes <- threadr::str_trim_many_spaces(sql_processes)
  
  # Get process keys
  df_processes <- databaser::db_get(con, sql_processes)
  
  # Check mapping table
  if (nrow(df_processes) == 0) {
    
    warning(
      "Check the `site` and `period` arguments, processes are not available...", 
      call. = FALSE
    )
    
    return(data.frame())
    
  }
  
  # Query database to get sites' data
  df <- import_by_process(
    con,
    process = df_processes$process, 
    summary = summary, 
    start = start, 
    end = end, 
    tz = tz, 
    valid_only = valid_only,
    date_end = date_end, 
    date_insert = date_insert, 
    site_name = site_name, 
    unit = unit,
    print_query = print_query
  )
  
  # Return empty data frame here if no observations
  if (nrow(df) == 0) return(df)
  
  # Drop all NAs for padding and reshaping
  df <- df[!is.na(df$value), ]
  
  # Groups for time-padding
  if (site_name) {
    
    site_variables <- c("site", "site_name")  
    
  } else {
    
    site_variables <- "site"
    
  }
  
  if (spread) {
    
    # Drop
    if (date_insert) df$date_insert <- NULL
    if (unit) df$unit <- NULL
    
    # Spread data
    df <- tryCatch({
      
      df %>%
        select(-process,
               -summary,
               -validity) %>%
        tidyr::spread(variable, value)
      
    }, error = function(e) {
      
      # Raise warning
      warning(
        "Data has been removed to honour `spread` argument...", 
        call. = FALSE
      )
      
      df %>%
        distinct(date,
                 site,
                 variable, 
                 .keep_all = TRUE) %>% 
        select(-process,
               -summary,
               -validity) %>%
        tidyr::spread(variable, value)
      
    })
    
    if (pad) {
      
      # Pad time-series
      df <- threadr::time_pad(df, interval = interval_pad, by = site_variables)
      
    }
    
  } else {
    
    if (pad) {
      
      # Pad time-series
      df <- threadr::time_pad(
        df, 
        interval = interval_pad, 
        by = c("process", "summary", site_variables, "variable")
      )
      
    }
    
  }
  
  # Clean R's names prefix, occurs with hydrocarbon variables
  names(df) <- stringr::str_replace(names(df), "^X", "")
  
  return(df)
  
}

# For testing of inputs
valid_periods <- c("source", "15_minute", "hour", "day", "month", "year")
