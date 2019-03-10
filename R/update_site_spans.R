#' Function to update the \code{date_start}, \code{date_end}, 
#' \code{observation_count}, and \code{variables_monitored} variables for a 
#' \code{`sites`} table in an smonitor database. 
#' 
#' Use \code{update_site_span} after \code{\link{update_process_spans}} because 
#' it queries the \code{`processes`} table rather than \code{`observations`}. 
#' 
#' @param con Database connection. 
#' 
#' @param variables_monitored Should the \code{variables_monitored} variable 
#' also be updated? 
#' 
#' @seealso \code{\link{update_process_spans}}, 
#' \code{\link{update_date_span_variables}}
#' 
#' @return Invisible \code{con}. 
#' 
#' @author Stuart K. Grange
#'
#' @export
update_site_spans <- function(con, variables_monitored = FALSE) {
  
  # Get data and transform
  df <- databaser::db_get(
    con, 
    "SELECT site,
    date_start, 
    date_end
    FROM processes"
  ) %>% 
    mutate(date_start = suppressWarnings(as.numeric(date_start)),
           date_end = suppressWarnings(as.numeric(date_end)))
  
  # Summarise
  df <- df %>% 
    group_by(site) %>% 
    summarise(date_start = min(date_start, na.rm = TRUE),
              date_end = max(date_end, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(date_start = ifelse(is.infinite(date_start), NA, date_start),
           date_end = ifelse(is.infinite(date_end), NA, date_end),
           date_start = stringr::str_replace_na(date_start),
           date_end = stringr::str_replace_na(date_end))
  
  # Build update statements
  sql <- stringr::str_c(
    "UPDATE sites
     SET date_start=", df$date_start, 
    ",date_end=", df$date_end, 
    " WHERE site='", df$site, "'"
  )
  
  # Make nulls
  sql <- stringr::str_replace_all(sql, "NA", "NULL")
  
  # Clean
  sql <- stringr::str_squish(sql)
  
  # Update variables to be null before insert
  databaser::db_execute(
    con, 
    "UPDATE sites SET date_start = NULL, date_end = NULL"
  )
  
  # Use statements
  databaser::db_execute(con, sql)
  
  # Update observation counts if they exist
  if ("observation_count" %in% databaser::db_list_variables(con, "sites")) {
    update_sites_observation_counts(con)
  }
  
  # Also update variables monitored
  if (variables_monitored) {
    update_variables_monitored(con)
  }
  
  return(invisible(con))
  
}


update_sites_observation_counts <- function(con) {
  
  # Get processes counts
  df_processes <- databaser::db_get(
    con, 
    "SELECT 
    site, 
    observation_count
    FROM processes
    ORDER BY site"
  )
  
  # Summarise
  df_processes_counts <- df_processes %>% 
    group_by(site) %>% 
    summarise(observation_count = sum(observation_count, na.rm = TRUE)) %>% 
    ungroup()
  
  # Set all to null before update
  databaser::db_execute(con, "UPDATE sites SET observation_count = NULL")
  
  # Update variable
  df_processes_counts %>% 
    databaser::build_update_statements("sites", ., where = "site", squish = TRUE) %>% 
    databaser::db_execute(con, .)
  
  # No return
  
}
