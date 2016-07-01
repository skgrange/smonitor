#' Function to import \code{`processes`} table from a \strong{smonitor} database. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' @param type Type of query to run; either \code{"full"} or \code{"minimal"}. 
#' 
#' @author Stuart K. Grange
#' 
#' @import stringr
#' 
#' @export
import_processes <- function(con, type = "full") {
  
  # Check
  types_allowed <- c("full", "minimal")
  
  if (!type %in% types_allowed)
    stop("'type' not recognised.", call. = FALSE)
  
  # Different queries
  if (type == "full") {
    
    # Get everything from processes and things from sites
    sql <- str_c("SELECT processes.*, 
                  sites.site_name, 
                  sites.region, 
                  sites.country,
                  sites.site_type
                  FROM processes
                  LEFT JOIN sites
                  ON processes.site = sites.site
                  ORDER BY processes.process")
    
  }
  
  if (type == "minimal") {
    
    # Minimal things
    sql <- str_c("SELECT processes.process, 
                 processes.site,
                 processes.variable,
                 processes.period,
                 sites.site_name, 
                 sites.site_type
                 FROM processes 
                 LEFT JOIN sites
                 ON processes.site = sites.site
                 ORDER BY processes.process")
    
  }
  
  # Clean
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Query
  df <- databaser::db_get(con, sql)
    
  # Return
  df
  
}
