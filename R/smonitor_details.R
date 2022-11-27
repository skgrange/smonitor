#' Function to get details for a \strong{smonitor} database. 
#' 
#' @param con Database connection for a \strong{smonitor} database. 
#' 
#' @param task A character vector to indicate what task is being (or has been) 
#' done. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
#' 
#' @export
smonitor_details <- function(con, task = NA_character_) {
  
  # Check if tables exist
  stopifnot(databaser::db_table_exists(con, c("sites", "processes", "observations")))
  
  tibble(
    system = threadr::hostname(),
    date = round(as.numeric(lubridate::now())),
    db_name = databaser::db_name(con),
    db_class = databaser::db_class(con),
    task = task,
    db_size = databaser::db_size(con, unit = "mb"),
    n_tables = length(databaser::db_list_tables(con)),
    n_sites = databaser::db_get(con, "SELECT COUNT(*) AS n FROM sites")$n,
    n_processes = databaser::db_get(con, "SELECT COUNT(*) AS n FROM processes")$n,
    n_observations = databaser::db_get(con, "SELECT COUNT(*) AS n FROM observations")$n
  )
  
}
