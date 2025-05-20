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
    operating_system = get_os_release_string(),
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
    purrr::map_dbl(~get_db_row_count(con, ., tables)) %>% 
    t() %>% 
    as_tibble()
  
  # Bind the two tibbles together
  df <- dplyr::bind_cols(df, df_counts)
  
  return(df)
  
}


# Also used in the sactivtyr package, could put this in databaser
get_db_row_count <- function(con, table, tables) {
  
  # Using numeric not integers to avoid 64-bit integer issues
  if (table %in% tables) {
    x <- glue::glue("SELECT COUNT(*) AS n FROM {table}") %>% 
      databaser::db_get(con, .) %>% 
      pull(n) %>% 
      as.numeric()
  } else {
    x <- NA_real_
  }
  
  return(x)
  
}


# Pulled from systemr
get_os_release_string <- function(add_prefix = TRUE) {
  
  # Get os name from R
  os_system_name <- stringr::str_to_lower(Sys.info()["sysname"])
  
  if (os_system_name == "windows") {
    
    # Get system info with system call
    df <- get_windows_system_info()
    
    # Build release string
    x <- stringr::str_c(df$value[1], " (", df$value[2], ")")
    
  } else if (os_system_name == "linux") {
    
    # Get release info by reading file
    df <- get_os_release("/etc/os-release")
    
    # Get the string that is desired
    x <- df %>% 
      filter(stringr::str_detect(variable, "(?i)pretty")) %>% 
      pull(value)
    
  }
  
  # Add prefix if desired
  if (add_prefix) {
    x <- stringr::str_c(os_system_name, ":", x)
  }
  
  return(x)
  
}


get_os_release <- function(file = "/etc/os-release") {
  
  if (fs::file_exists(file)) {
    
    # Read release file as text
    text <- readLines(file)
    
    # Format to a tibble
    df <- text %>% 
      stringr::str_remove_all('"') %>% 
      stringr::str_split_fixed("=", n = 2) %>% 
      as_tibble(.name_repair = "minimal") %>% 
      purrr::set_names(c("variable", "value"))
    
  } else {
    df <- tibble()
  }
  
  return(df)
  
}


get_windows_system_info <- function() {
  
  # Run programme, a slow call and `ver` is not available it seems
  list_run <- processx::run("systeminfo")
  
  # Extract and format return
  list_run %>% 
    .[["stdout"]] %>% 
    stringr::str_split_1("\n") %>% 
    stringr::str_subset("^OS Name|^OS Version") %>% 
    stringr::str_squish() %>% 
    stringr::str_split_fixed(": ", n = 2) %>% 
    as_tibble(.name_repair = "minimal") %>% 
    purrr::set_names(c("variable", "value"))
  
}
