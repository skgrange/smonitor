#' Function to import observational data from a \strong{smonitor} database. 
#' 
#' \code{import_by_process} is considered the primary data importer for 
#' \strong{smonitor}. 
#' 
#' @param con A \strong{smonitor} database connection. 
#' 
#' @param process A vector of processes.
#' 
#' @param summary A vector of summaries. If summary is \code{NA} (the default), 
#' then only the \code{process} will be used in the \code{WHERE} clause. 
#' 
#' @param start What is the start date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and
#' will floor-rounded. 
#' 
#' @param end What is the end date of data to be returned? Ideally, the 
#' date format should be \code{yyyy-mm-dd}, but the UK locale convention of 
#' \code{dd/mm/yyyy} will also work. Years as strings or integers work too and 
#' will be ceiling-rounded. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}. 
#' 
#' @param valid_only Should invalid observations be filtered out? Default is 
#' \code{TRUE}. Valid observations are considered to be those with the validity
#' variable being \code{1} or missing (\code{NULL} or \code{NA}). This argument
#' will be set to \code{FALSE} if \code{set_invalid_values} is used.
#' 
#' @param set_invalid_values Should invalid observations be set to \code{NA}? 
#' See \code{\link{set_invalid_values}} for details and this argument will set
#' the \code{valid_only} argument to \code{FALSE}. 
#' 
#' @param date_end Should the return include the \code{date_end} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param date_insert Should the return include the \code{date_insert} variable? 
#' Default is \code{FALSE}. 
#' 
#' @param site_name Should the return include the \code{site_name} variable? 
#' Default is \code{TRUE}. 
#' 
#' @param unit Should the processes' units be included in the return? 
#' 
#' @param query_by_process Should observations be queried process by process? 
#' This will avoid sending large numbers of processes to the database within 
#' \code{IN} clauses. 
#' 
#' @param warn Should the functions raise warnings? 
#' 
#' @param arrange_by How should the returned table be arranged? Can be either
#' \code{"process"} or \code{"date"}. 
#' 
#' @param progress If \code{query_by_process} is \code{TRUE}, should a progress
#' bar be displayed? 
#' 
#' @return Tibble containing decoded observational data with correct data types. 
#' 
#' @seealso \code{\link{import_by_site}} for a higher-level importing function
#' 
#' @author Stuart K. Grange
#' 
#' @examples 
#' \dontrun{
#'
#' # Simple usage, all summaries for a process will be returned
#' data_example <- import_by_process(con, process = 400)
#' 
#' # Many processes
#' data_example <- import_by_process(con, process = 1:20)
#' 
#' # Simple usage, a single summary will be returned
#' data_example <- import_by_process(con, process = 400, summary = 1)
#' 
#' }
#' 
#' @export
import_by_process <- function(con, process = NA, summary = NA, start = 1969, 
                              end = NA, tz = "UTC", valid_only = TRUE, 
                              set_invalid_values = FALSE, date_end = TRUE, 
                              date_insert = FALSE, site_name = TRUE, 
                              unit = TRUE, query_by_process = FALSE,
                              warn = TRUE, arrange_by = "process",
                              progress = FALSE) {
  
  # Check inputs
  if (is.na(process[1])) {
    cli::cli_abort("The `process` argument must be used.")
  }
  
  # Can only be one of two things
  arrange_by <- stringr::str_to_lower(arrange_by)
  stopifnot(arrange_by %in% c("process", "date"))
  
  # Check for sql wildcards
  databaser::db_wildcard_check(process)
  databaser::db_wildcard_check(summary)
  
  # Parse date arguments
  start <- threadr::parse_date_arguments(start, "start")
  end <- threadr::parse_date_arguments(end, "end")
  
  # Push to last instant in day
  if (lubridate::hour(end) == 0) end <- end + (threadr::seconds_in_a_day() - 1)
  
  # For sql
  start <- as.numeric(start)
  end <- as.numeric(end)
  process_collapsed <- stringr::str_c(process, collapse = ",")
  summary_collapsed <- stringr::str_c(summary, collapse = ",")
  
  # Get table to link processes with sites
  df_processes <- import_by_process_process_table(
    con, 
    process = process_collapsed, 
    site_name = site_name, 
    unit = unit
  )
  
  # Check for data
  if (nrow(df_processes) == 0) {
    if (warn) {
      cli::cli_warn(
        "Process(s) not found in the database, no data has been returned..."
      )
    }
    # Return empty tibble
    return(tibble())
  }
  
  # Get observations
  if (!query_by_process) {
    # Use collapsed vectors here
    df <- import_by_process_observation_table(
      con, 
      process = process_collapsed, 
      summary = summary_collapsed, 
      start = start,
      end = end,
      date_end = date_end, 
      date_insert = date_insert
    )
  } else {
    df <- import_by_process_observation_table_by_process(
      con, 
      process = process, 
      summary = summary, 
      start = start,
      end = end,
      date_end = date_end, 
      date_insert = date_insert,
      progress = progress
    )
  }
  
  # Check for data
  if (nrow(df) == 0) {
    if (warn) {
      cli::cli_warn(
        "The database has been queried but no data has been returned..."
      )
    }
    # Return empty tibble
    return(tibble())
  }
  
  # Switch logic
  if (valid_only & set_invalid_values) {
    if (warn) {
      cli::cli_warn(
        "Both `valid_only` and `set_invalid_values` are `TRUE`, only `set_invalid_values` will be honoured..."
      )
    }
    valid_only <- FALSE
  }
  
  # Remove invalid observations, may move to sql at some point
  if (valid_only) {
    df <- filter(df, validity %in% 1L:3L | is.na(validity))
  }
  
  # Probably better to set invalid value to missing rather than to use 
  # `valid_only`
  if (set_invalid_values) {
    df <- mutate(
      df, value = set_invalid_values(value, validity, include_zero = TRUE)
    )
  }
  
  # Check for data
  if (nrow(df) == 0) {
    if (warn) {
      cli::cli_warn("The database contains no valid observations...")
    }
    # Return empty tibble
    return(tibble())
  }
  
  # Join process and site data
  df <- left_join(df, df_processes, by = join_by(process))
  
  # Parse dates
  df$date <- threadr::parse_unix_time(df$date, tz = tz)
  
  # Often not returned
  if (date_insert) {
    df$date_insert <- threadr::parse_unix_time(df$date_insert, tz = tz)
  }
  
  # As numeric force is for 64 bit integer issues
  if (date_end) {
    df$date_end <- threadr::parse_unix_time(as.numeric(df$date_end), tz = tz)
  }
  
  # Arrange observations
  if (arrange_by == "process") {
    df <- arrange(df, process, date)
  } else if (arrange_by == "date") {
    df <- arrange(df, date, process)
  }
  
  # And make a nice variable order
  df <- relocate(
    df, 
    dplyr::matches("date_insert"),
    dplyr::matches("date"), 
    dplyr::matches("date_end"), 
    dplyr::matches("site"), 
    dplyr::matches("site_name"), 
    dplyr::matches("variable"), 
    dplyr::matches("process"), 
    dplyr::matches("summary"),
    dplyr::matches("validity"),
    dplyr::matches("unit"),
    dplyr::matches("value")
  )
  
  return(df)
  
}


import_by_process_process_table <- function(con, process, site_name, unit) {
  
  # Link processes to sites
  sql_processes <- stringr::str_c(
    "SELECT processes.process, 
    processes.site,
    processes.variable,
    processes.unit,
    sites.site_name
    FROM processes 
    LEFT JOIN sites
    ON processes.site = sites.site
    WHERE process IN (", process, ")"
  )
  
  # Clean
  df <- sql_processes %>% 
    stringr::str_squish() %>% 
    databaser::db_get(con, .)
  
  # Drop variables
  if (!site_name) df$site_name <- NULL
  if (!unit) df$unit <- NULL
  
  return(df)
  
}


import_by_process_observation_table <- function(con, process, summary, start, 
                                                end, date_end, date_insert) {
  
  if (is.na(summary[1])) {
    
    sql_observations <- stringr::str_c(
      "SELECT observations.date_insert, 
      observations.date,
      observations.date_end,
      observations.value,
      observations.process,
      observations.summary,
      observations.validity
      FROM observations
      WHERE observations.process IN (", process, ")
      AND observations.date BETWEEN ", start, " AND ", end
    )
    
  } else {
    
    sql_observations <- stringr::str_c(
      "SELECT observations.date_insert, 
      observations.date,
      observations.date_end,
      observations.value,
      observations.process,
      observations.summary,
      observations.validity
      FROM observations
      WHERE observations.process IN (", process, ")
      AND observations.date BETWEEN ", start, " AND ", end, "
      AND observations.summary IN (", summary, ")"
    )
    
  }
  
  # Drop date_end from query
  if (!date_end) {
    sql_observations <- stringr::str_remove(
      sql_observations, 
      "observations.date_end,"
    )
  }

  # Drop date_insert from query
  if (!date_insert) {
    sql_observations <- stringr::str_remove(
      sql_observations, 
      "observations.date_insert,"
    )
  }
    
  # Clean sql
  sql_observations <- stringr::str_squish(sql_observations)
  
  # Get observations
  df <- databaser::db_get(con, sql_observations)
  
  return(df)
  
}


import_by_process_observation_table_by_process <- function(con, process, 
                                                           summary, start, 
                                                           end, date_end, 
                                                           date_insert,
                                                           progress) {
  
  # Build select sql statements for each process and summary pair
  sql <- build_granular_select_observation_sql(
    process, summary, start, end, date_end = date_end, date_insert = date_insert
  )
  
  # Query database n times
  df <- sql %>% 
    purrr::map(~databaser::db_get(con, .), .progress = progress) %>% 
    purrr::list_rbind()
  
  return(df)
  
}


build_granular_select_observation_sql <- function(process, summary, start, end,
                                                  date_end, date_insert) {
  
  # Check length of date filters
  stopifnot(length(start) == 1L, length(end) == 1L)
  
  # Build a mapping table
  df_map <- tidyr::expand_grid(
    process,
    summary,
    start,
    end
  )
  
  # Build the many sql statements
  sql <- purrr::pmap(df_map, build_granular_select_observation_sql_worker) %>% 
    purrr::flatten_chr()
  
  # Drop date_end if desired
  if (!date_end) {
    sql <- stringr::str_remove(sql, "observations.date_end,")
  }
  
  # Dop date_insert if desired
  if (!date_insert) {
    sql <- stringr::str_remove(sql, "observations.date_insert,")
  }
  
  # Clean the statements a bit
  sql <- stringr::str_squish(sql)
  
  return(sql)
  
}


build_granular_select_observation_sql_worker <- function(process, summary, start, 
                                                         end) {
  
  sql <- stringr::str_glue(
    "SELECT observations.date_insert,
    observations.date,
     observations.date_end,
     observations.value,
     observations.process,
     observations.summary,
     observations.validity
     FROM observations
     WHERE observations.process = {process}
     AND observations.date BETWEEN {start} AND {end}"
  )
  
  # Add summary if passed
  if (!is.na(summary)) {
    sql <- stringr::str_glue(
      "{sql}
      AND summary = {summary}"
    )
  }
  
  return(sql)
  
}
