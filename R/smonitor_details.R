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
  
  # Get all tables
  tables <- databaser::db_list_tables(con)
  
  # Check if tables exist
  stopifnot(c("sites", "processes", "observations") %in% tables)
  
  # Get package versions as a single string
  package_versions <- threadr::get_package_version(
    c(
      "R", "dplyr", "threadr", "icostoolr", "databaser", "smonitor", "ssensors", 
      "decentlabr"
    ), 
    as_vector = TRUE
  ) %>% 
    stringr::str_c(collapse = "; ")
  
  # Get high level database and details
  df <- tibble(
    system = threadr::hostname(),
    operating_system = stringr::str_to_lower(Sys.info()["sysname"]),
    date = round(as.numeric(lubridate::now())),
    db_name = databaser::db_name(con),
    db_class = databaser::db_class(con),
    package_versions = package_versions,
    task = task,
    db_size = databaser::db_size(con, unit = "mb"),
    n_tables = length(tables)
  )
  
  # Get row counts of various tables
  tables_to_count <- c(
    "sites", "processes", "observations", "sensors", "cylinder_test_summaries", 
    "deployments_sensors", "deployments_cylinders", "r_objects"
  )
  
  # Get row counts
  df_counts <- tables_to_count %>% 
    purrr::set_names(stringr::str_c("n_", .)) %>% 
    purrr::map_int(~get_db_row_count(con, ., tables)) %>% 
    t() %>% 
    as_tibble()
  
  # Bind the two tibbles together
  df <- dplyr::bind_cols(df, df_counts)
  
  return(df)
  
}


# Also used in the sactivtyr package, could put this in databaser
get_db_row_count <- function(con, table, tables) {
  
  if (table %in% tables) {
    x <- glue::glue("SELECT COUNT(*) AS n FROM {table}") %>% 
      databaser::db_get(con, .) %>% 
      pull(n)
  } else {
    x <- NA_integer_
  }
  
  return(x)
  
}
