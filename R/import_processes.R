#' Function to import \code{`processes`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_processes <- function(con, extra = TRUE) {
  
  
  if (extra) {
    
    df <- tryCatch({
      
      threadr::db_get(con, "SELECT processes.*, 
                          sites.site_name, 
                          sites.region, 
                          sites.country,
                          sites.site_type
                          FROM processes
                          LEFT JOIN sites
                          ON processes.site = sites.site
                          ORDER BY processes.process")
      
    }, error = function(e) {
      
      # No country here, a temp measure
      threadr::db_get(con, "SELECT processes.*, 
                          sites.site_name, 
                          sites.region, 
                          sites.site_type
                          FROM processes
                          LEFT JOIN sites
                          ON processes.site = sites.site
                          ORDER BY processes.process")
      
    })
    
  } else {
    
    df <- threadr::db_get(con, "SELECT processes.process, 
                                processes.site,
                                processes.variable,
                                processes.period,
                                processes.group_code,
                                processes.data_source,
                                sites.site_name, 
                                sites.region, 
                                sites.site_type
                                FROM processes 
                                LEFT JOIN sites
                                ON processes.site = sites.site
                                ORDER BY process")
    
  }
  
  # # Only a few variables
  # if (!extra)
  #   df <- df[, c("process", "site_name", "site", "variable", "period")]
  
  # Return
  df
  
}