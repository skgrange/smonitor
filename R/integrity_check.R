#' Function to test data-model integrity of an \strong{smonitor} database. 
#' 
#' \code{integrity_check} tests if the foreign keys are accounted for in the
#' \strong{smonitor} tables.
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @export
integrity_check <- function(con) {
  
  # Check processes in `observations`
  processes_process <- threadr::db_get(con, "SELECT DISTINCT process 
                                       FROM processes")$process
  
  observations_process <- threadr::db_get(con, "SELECT DISTINCT process 
                                          FROM observations")$process
  
  process_difference <- observations_process %in% processes_process
  
  if (all(process_difference)) {
    
    message("Process keys are correct in `observations`")
    
  } else {
    
    message("Process keys are not correct in `observations`")
    
  }
  
  
  # Check sites in `processes`
  sites_site <- threadr::db_get(con, "SELECT DISTINCT site 
                                      FROM sites")$site
  
  processes_site <- threadr::db_get(con, "SELECT DISTINCT site 
                                          FROM processes")$site
  
  site_difference <- processes_site %in% sites_site
  
  if (any(site_difference)) {
    
    message("Site keys are correct in `processes`")
    
  } else {
    
    message("Site keys are not correct in `processes`")
    
  }
  
  
  # Check summaries in `aggregations`
  aggregations_summary <- threadr::db_get(con, "SELECT DISTINCT summary
                                                FROM aggregations")$summary
  
  observations_summary <- threadr::db_get(con, "SELECT DISTINCT summary
                                                FROM observations")$summary
  
  summary_difference <- observations_summary %in% aggregations_summary
  
  if (all(summary_difference)) {
    
    message("Summary keys are correct in `observations`")
    
  } else {
    
    message("Summary keys are not correct in `observations`")
    
  }
  
}

