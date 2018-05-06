#' Function to import a local copy of the \strong{smonitor} Europe \code{sites} 
#' table. 
#' 
#' @param file Local file name. 
#' 
#' @param tz Time zone for dates. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame.
#' 
#' @export
import_europe_sites_local <- function(file = NA, tz = "UTC") {
  
  # Default file name
  if (is.na(file))
    file <- "~/Dropbox/R/smonitor_europe/data/copied_database_tables/sites_table.rds"
  
  # Load data
  df <- readRDS(file)
  
  # Parse dates
  df$date_start <- threadr::parse_unix_time(df$date_start, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  return(df)
  
}



#' Function to import a local copy of the \strong{smonitor} Europe 
#' \code{processes} table. 
#' 
#' @param file Local file name. 
#' 
#' @param tz Time zone for dates. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame.
#' 
#' @export
import_europe_processes_local <- function(file = NA, tz = "UTC") {
  
  # Default file name
  if (is.na(file))
    file <- "~/Dropbox/R/smonitor_europe/data/copied_database_tables/processes_table.rds"
  
  # Load data
  df <- readRDS(file)
  
  # Parse dates
  df$date_start <- threadr::parse_unix_time(df$date_start, tz = tz)
  df$date_end <- threadr::parse_unix_time(df$date_end, tz = tz)
  
  return(df)
  
}
