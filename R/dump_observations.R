#' Function to export an \strong{smonitor}'s \code{`observations`} table as 
#' \code{.rds} objects. 
#' 
#' @param con Database connection. 
#' @param n Number of processes to query and save for each iteration. Default is
#' \code{100}. 
#' @param output Output directory to where \code{.rds} objects should be saved. 
#' @param progress Type of progress bar to display. Default is \code{"time"}. 
#' 
#' @author Stuart K. Grange
#' 
#' @examples 
#' \dontrun{
#' dump_observations(con, n = 5, output = "~/Desktop/")
#' }
#' 
#' @export
dump_observations <- function(con, n = 100, output, progress = "time") {
  
  # Get process vector
  processes <- databaser::db_get(con, "SELECT process 
                                       FROM processes 
                                       ORDER BY process")[, 1]
  
  # Split into a list
  list_processes <- threadr::split_nrow(processes, n)
  
  # Do for each group of processes
  plyr::l_ply(list_processes, function(x) 
    dump_observations_worker(con, x, output = output), .progress = progress)
  
  # No return
  
}


# The worker
dump_observations_worker <- function(con, process, output) {
  
  # Get things for file name
  process_min <- min(process)
  process_max <- max(process)
  
  # Build sql
  process <- stringr::str_c(process, collapse = ",")
  
  sql <- stringr::str_c("SELECT * 
                        FROM observations  
                        WHERE process IN (", process, ")")
  
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Get data
  df <- databaser::db_get(con, sql)
  
  file_name <- stringr::str_c("observations_", process_min, "_to_", process_max, 
                              ".rds")
  
  file_name <- file.path(output, file_name)
  
  # Export file
  saveRDS(df, file_name)
  
}
