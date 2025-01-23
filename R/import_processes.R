#' Function to import \code{`processes`} table from a \strong{smonitor} 
#' database. 
#' 
#' @param con Database connection to a \strong{smonitor} database.
#' 
#' @param with_sensors If the \strong{smonitor} data model contains a 
#' \code{`sensors`} table (or \code{`monitors`} and \code{`deployments`} tables), 
#' should additional information be joined to the \code{`processes`} return? 
#' 
#' @param with_sites_join Should additional variables from \code{`sites`} be 
#' joined to the \code{`processes`} table? 
#' 
#' @param tz What time zone should the \code{date_start} and \code{date_end}
#' variables be represented as?
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
#' 
#' @seealso \code{\link{import_sites}},  \code{\link{import_by_process}}
#' 
#' @examples 
#' \dontrun{
#' 
#' # Import processes from a smonitor database
#' data_processes <- import_processes(con)
#' 
#' }
#' 
#' @export
import_processes <- function(con, with_sensors = TRUE, with_sites_join = TRUE, 
                             tz = "UTC") {
  
  # Check if the tables exist
  stopifnot(databaser::db_table_exists(con, c("processes", "sites")))
  
  # Build the main sql query
  if (with_sites_join) {
    # Get everything from processes and some things from sites
    sql <- "
      SELECT processes.*, 
      sites.site_name,
      sites.country,
      sites.site_type
      FROM processes
      LEFT JOIN sites
      ON processes.site = sites.site
      ORDER BY processes.process
   "
  } else {
    # Query `processes` and only join the extra site_name variable from `sites`
    sql <- "
      SELECT processes.*,
      sites.site_name
      FROM processes
      LEFT JOIN sites
      ON processes.site = sites.site
      ORDER BY process
    "
  }
  
  # Query database and parse dates
  df <- sql %>% 
    stringr::str_squish() %>% 
    databaser::db_get(con, .) %>% 
    mutate(date_start = parse_numeric_dates(date_start, tz = tz),
           date_end = parse_numeric_dates(date_end, tz = tz)) %>% 
    relocate(process,
             site,
             site_name)
  
  # `date_insert` is not present all the time so only parse if it exists
  if ("date_insert" %in% names(df)) {
    df <- mutate(df, date_insert = parse_numeric_dates(date_insert, tz = tz))
  }
  
  # `date_calibration` is not present all the time so only parse if it exists
  if ("date_calibration" %in% names(df)) {
    df <- mutate(
      df, date_calibration = parse_numeric_dates(date_calibration, tz = tz)
    )
  }
  
  # Join some additional sensor things to table too
  # The ssensors data model
  if (with_sensors && databaser::db_table_exists(con, "sensors")) {
    
    df_sensors <- databaser::db_get(
      con, 
      "SELECT sensor_id,
      sensor_type
      FROM sensors
      ORDER BY sensor_id"
    )
    
    # Join `sensors` data to `processes`
    df <- df %>% 
      left_join(df_sensors, by = join_by(sensor_id)) %>% 
      relocate(sensor_type,
               .after = sensing_element_id)
    
  }
  
  # The extended monitors data model
  if (
    all(
      with_sensors, databaser::db_table_exists(con, c("monitors", "deployments"))
    )
  ) {
    
    # Get extra deployment and monitor variables
    df_deployment <- stringr::str_glue(
      "SELECT process,
      processes.deployment,
      deployments.monitor,
      monitors.monitor_type
      FROM processes
      LEFT JOIN deployments
      ON processes.deployment = deployments.deployment
      LEFT JOIN monitors
      ON deployments.monitor = monitors.monitor"
    ) %>% 
      databaser::db_get(con, .)
    
    # Join deployment and monitor data to `processes`
    df <- df %>% 
      left_join(df_deployment, by = join_by(process, deployment)) %>% 
      relocate(monitor,
               monitor_type,
               .after = deployment)
    
  }
  
  return(df)
  
}


parse_numeric_dates <- function(x, tz) {
  
  # Warning suppression is for when elements are missing
  suppressWarnings(
    x %>% 
      as.numeric() %>% 
      threadr::parse_unix_time(tz = tz)
    
  )
  
}
