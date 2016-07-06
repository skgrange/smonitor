#' Function to import European data from an \strong{smonitor} database by
#' process or site. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param process A vector of processes.
#' 
#' @param site A site code such as \code{"betmeu1"}. 
#' 
#' @param start What is the start date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and
#' will floor-rounded. 
#' 
#' @param end What is the end date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and 
#' will be ceiling-rounded. 
#' 
#' @param period Averaging period. Default is \code{"hour"}. 
#' 
#' @param pad Should the time-series be padded to ensure all dates in the 
#' observation period are present? Default is \code{TRUE}. 
#' 
#' @param spread Should the data frame take the wider format resulting from
#' spreading the data? Default is \code{TRUE}. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param valid_only Should only valid data be returned? Default is \code{FALSE}. 
#' 
#' @param date_end Should the return include the \code{date_end} variable? 
#' Default is \code{TRUE}.
#' 
#' @param date_insert Should the return include the \code{date_insert} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param site_name Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_europe <- function(con, process = NA, site = NA, start = 1969, end = NA, 
                          period = "hour", pad = TRUE, spread = TRUE, tz = "UTC", 
                          valid_only = TRUE, date_end = FALSE, 
                          date_insert = FALSE, site_name = TRUE) {
  
  
  if (is.na(process[1]) & is.na(site[1])) 
    stop("'process' or 'site' must be used", call. = FALSE)
  
  # Use different functions for importing
  if (!is.na(process[1]) & is.na(site[1])) {
    
    df <- import_any(con, process, summary = NA, start = start, end = end,
                     tz = tz, valid_only = valid_only, date_end = date_end, 
                     date_insert = date_insert, site_name = site_name)
    
  }
  
  if (!is.na(site[1]) & is.na(process[1])) {
    
    df <- import_by_site(con, site, start = start, end = end, period = period,
                         valid_only = valid_only, pad = pad, tz = tz, 
                         spread = spread, europe = TRUE, date_end = date_end, 
                         date_insert = date_insert, site_name = site_name)
    
  }
  
  # Return
  df
  
}
