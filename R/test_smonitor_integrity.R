#' Function to test the integrity of a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @export
test_smonitor_integrity <- function(con) {
  
  date_start <- lubridate::now()
  database_name <- databaser::db_name(con, extension = FALSE)
  database_size <- databaser::db_size(con)
  
  message(stringr::str_c("This database is ", database_name, "..."))
  message(stringr::str_c("The database size is ", database_size, " (MB)..."))
  
  tables <- databaser::db_list_tables(con)
  
  # Test for needed tables
  if (!all(c("sites", "processes", "observations") %in% tables))
    stop("This database is not a smonitor database...", call. = FALSE)
  
  # Test for the core tables
  tables_missing <- setdiff(
    c("sites", "processes", "observations", "summaries", "aggregations", 
      "invalidations", "calibrations"
    ), 
    tables
  )
  
  if (length(tables_missing) != 0) {

    message("There are missing smonitor core tables...")
    
    # For return
    tables_missing <- stringr::str_c(tables_missing, collapse = "; ")
    
  } else {
    
    message("There are no missing smonitor tables...")
    
    # For return
    tables_missing <- NA
    
  }
  
  sites <- databaser::db_get(con, "SELECT site FROM sites ORDER BY site")[, 1]
  sites_duplicated <- any(duplicated(sites))
  
  if (sites_duplicated) {
    
    message("There are duplicated sites in `sites`...")
    
  } else {
    
    message("There are no duplicated sites in `sites`...")
    
  }
  
  processes <- databaser::db_get(con, "SELECT process FROM processes ORDER BY process")[, 1]
  processes_duplicated <- any(duplicated(processes))
  
  if (sites_duplicated) {
    
    message("There are duplicated processes in `processes`...")
    
  } else {
    
    message("There are no duplicated processes in `processes`...")
    
  }
  
  sites_with_processes <- databaser::db_get(con, "SELECT DISTINCT site FROM processes ORDER BY site")[, 1]
  sites_with_no_processes <- setdiff(sites, sites_with_processes)
  
  if (length(sites_with_no_processes) != 0) {
    
    message("There are sites which do not have processes...")
    sites_with_no_processes <- stringr::str_c(sites_with_no_processes, collapse = "; ")
    
  } else {
    
    message("All sites have processes...")
    sites_with_no_processes <- NA
    
  }
  
  
  dates <- databaser::db_get(con, "SELECT date FROM observations WHERE date IS NULL")[, 1]
  dates_missing <- ifelse(length(dates) != 0, TRUE, FALSE)
  
  if (dates_missing) {
    
    message("There are missing dates in `observations`...")
    
  } else {
    
    message("There are no missing dates in `observations`...")
    
  }
  
  
  processes_observations <- databaser::db_get(con, "SELECT process FROM observations WHERE process IS NULL")[, 1]
  processes_observations_missing <- ifelse(length(processes_observations) != 0, TRUE, FALSE)
  
  if (processes_observations_missing) {
    
    message("There are missing processes in `observations`...")
    
  } else {
    
    message("There are no missing processes in `observations`...")
    
  }
  
  summary_observations <- databaser::db_get(con, "SELECT summary FROM observations WHERE summary IS NULL")[, 1]
  summary_observations_missing <- ifelse(length(summary_observations) != 0, TRUE, FALSE)
  
  if (summary_observations_missing) {
    
    message("There are missing summaries in `observations`...")
    
  } else {
    
    message("There are no missing summaries in `observations`...")
    
  }
  
  # Build return
  df <- data.frame(
    date = as.numeric(date_start),
    database_name,
    database_size,
    tables_missing,
    sites_duplicated,
    processes_duplicated,
    sites_with_no_processes,
    dates_missing,
    processes_observations_missing,
    summary_observations_missing,
    stringsAsFactors = FALSE
  )
  
  return(df)
  
}
