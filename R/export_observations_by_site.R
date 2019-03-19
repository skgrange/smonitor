#' Function to export tidy tabular files from a \strong{smonitor} database which 
#' can be shared and easily used.
#' 
#' \code{export_observations_by_site} will create site-based data files.
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection to a \strong{smonitor} database. 
#' 
#' @param df A mapping data frame containing \code{site} and \code{process} 
#' variables. 
#' 
#' @param start Start date of observations to query from database. 
#' 
#' @param end End date of observations to query from database. 
#' 
#' @param site_name Should the site name variable be included in the return?
#' 
#' @param directory_output The directory to export files to.
#' 
#' @param file_name_prefix Prefix to use for the exported file names. The default
#' is \code{"air_quality_data_site_"}. 
#' 
#' @param file_type Type of files to export. Can be either \code{".csv.gz"} or 
#' \code{".rds"}. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Invisible \code{con} and exported files.
#' 
#' @export
export_observations_by_site <- function(con, df, start = 1960, end = NA, 
                                        site_name = FALSE, directory_output, 
                                        file_name_prefix = NA_character_,
                                        file_type = ".csv.gz", verbose = FALSE) {
  
  # Check data frame input
  stopifnot("process" %in% names(df))
  stopifnot("site" %in% names(df))
  
  # Check file type argument
  file_type <- stringr::str_to_lower(file_type)
  stopifnot(file_type %in% c(".rds", ".csv.gz"))
  
  # Default file name prefix
  file_name_prefix <- if_else(
    is.na(file_name_prefix), 
    "air_quality_data_site_", file_name_prefix
  )
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  
  # Default to this year
  if (is.na(end)) end <- lubridate::year(lubridate::now())
  end <- threadr::parse_date_arguments(end, "end")
  
  # For function
  start <- format(start)
  end <- format(end)
  
  # Do by site
  df %>% 
    split(.$site, drop = TRUE) %>% 
    purrr::walk(
      ~export_observations_by_site_worker(
        con, 
        df = .x,
        start = start,
        end = end, 
        site_name = site_name,
        directory_output = directory_output,
        file_name_prefix = file_name_prefix,
        file_type = file_type,
        verbose = verbose
      )
    )
  
  return(invisible(con))
  
}


# Define the worker
export_observations_by_site_worker <- function(con, df, start, end, site_name,
                                               directory_output, file_name_prefix, 
                                               file_type, verbose) {
  
  # Query database for observations
  if (verbose) {
    message(threadr::date_message(), "Querying database for site `", df$site[1], "`...")
  }
  
  df <- import_by_process(
    con, 
    process = df$process,
    summary = NA,
    start = start,
    end = end,
    date_end = TRUE,
    site_name = site_name,
    valid_only = FALSE
  )
  
  if (nrow(df) != 0) {
    
    if (verbose) {
      message(threadr::date_message(), "Exporting `", file_type, "` data file...")
    }
    
    # Build output file name, no extension yet
    file_output <- stringr::str_c(file_name_prefix, df$site[1])
    file_output <- file.path(directory_output, file_output)
    file_output <- stringr::str_c(file_output, file_type)
    
    # Export
    if (file_type == ".rds") {
      saveRDS(df, file_output)
    } else if (file_type == ".csv.gz") {
      readr::write_csv(df, file_output)
    }
    
  } else{
    
    if (verbose) {
      message(threadr::date_message(), "No data returned from database, not exporting...")
    }
    
  }
  
  return(invisible(con))
  
}
