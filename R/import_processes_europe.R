#' Function to import \code{`processes`} table from the European 
#' \strong{smonitor} database. 
#' 
#' \code{import_processes_europe} is an optimised version of 
#' \code{import_processes} which includes a regular expression filter to speed
#' up the retreval of processes because the table is larger than normal. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' @param country_code A two digit ISO country code used for a regular
#' expression \code{WHERE} clause. This will usually be parsed from site codes. 
#' If \code{country_code} is \code{NA}, all ISO country codes in the database 
#' will be used. 
#' 
#' @seealso \code{\link{import_processes}}
#' 
#' @author Stuart K. Grange
#' 
#' @import stringr
#' 
#' @export
import_processes_europe <- function(con, country_code = NA) {
  
  if (is.na(country_code)) {
    
    # Use all codes in database
    country_code <- import_country_codes(con)$country_code
    
  }
  
  # Ensure some things
  country_code <- str_to_lower(country_code)
  
  # For sql
  country_code <- str_c(country_code, collapse = "|")
  
  # Build statement
  sql <- str_c("SELECT processes.process, 
               processes.site,
               processes.variable,
               processes.period,
               processes.date_start,
               processes.date_end,
               processes.group_code,
               processes.data_source,
               sites.site_name,
               sites.region,
               sites.country,
               sites.site_type,
               sites.site_area
               FROM processes
               LEFT JOIN sites
               ON processes.site = sites.site
               WHERE sites.site SIMILAR TO '(", country_code, ")%'
               ORDER BY processes.process")
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query database
  df <- databaser::db_get(con, sql)
  
  # Return
  df
  
}


#' @export
import_country_codes <- function(con) {
  
  databaser::db_get(con, "SELECT DISTINCT SUBSTRING (site, 1, 2) AS country_code
                          FROM sites 
                          ORDER BY country_code")
  
}
