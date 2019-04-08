#' Function to update the \code{date_start}, \code{date_end}, 
#' \code{observation_count}, \code{variables_monitored}, and \code{"data_source"}
#'  variables for a \code{`sites`} table in an \strong{smonitor} database. 
#' 
#' Use \code{update_site_span} after \code{\link{update_process_spans}} because 
#' it queries the \code{`processes`} table rather than \code{`observations`}. 
#' 
#' @param con \strong{smonitor} database connection. 
#' 
#' @param site Vector of sites to update. 
#' 
#' @seealso \code{\link{update_process_spans}}, 
#' \code{\link{update_date_span_variables}}
#' 
#' @return Invisible \code{con}. 
#' 
#' @author Stuart K. Grange
#'
#' @export
update_site_spans <- function(con, site = NA) {
  
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
  
  # Filter to sites
  if (!is.na(site[1])) df <- filter(df, site %in% !!site)
  
  # Summarise by site
  df <- df %>% 
    group_by(site) %>% 
    summarise(date_start = min(date_start, na.rm = TRUE),
              date_end = max(date_end, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(date_start = ifelse(is.infinite(date_start), NA, date_start),
           date_end = ifelse(is.infinite(date_end), NA, date_end),
           date_start = stringr::str_replace_na(date_start),
           date_end = stringr::str_replace_na(date_end))
  
  # Update variables to be null before insert
  if (!is.na(site[1])) {
    
    site_collapsed <- site %>% 
      stringr::str_c("'", ., "'") %>% 
      stringr::str_c(collapse = ",")
    
    sql_set_null <- stringr::str_c(
      "UPDATE sites
      SET date_start = NULL, 
      date_end = NULL 
      WHERE site IN (", site_collapsed, ")"
    ) 
    
  } else {
    
    # All sites
    sql_set_null <- "
      UPDATE sites 
      SET date_start = NULL, 
      date_end = NULL
    "
    
  }
  
  # Build update statements
  sql_update <- stringr::str_c(
    "UPDATE sites
     SET date_start = ", df$date_start, 
    ", date_end = ", df$date_end, 
    " WHERE site = '", df$site, "'"
  ) %>% 
    stringr::str_replace_all("NA", "NULL")
  
  # Use statements
  c(sql_set_null, sql_update) %>% 
    stringr::str_squish() %>% 
    databaser::db_execute(con, .)
  
  # Update observation counts
  if ("observation_count" %in% databaser::db_list_variables(con, "sites")) {
    update_sites_observation_counts(con, site = site)
  }
  
  # Update variables monitored
  if ("variables_monitored" %in% databaser::db_list_variables(con, "sites") && 
      "observation_count" %in% databaser::db_list_variables(con, "sites")) {
    update_variables_monitored(con, site = site)
  }
  
  # Update data sources
  if ("data_source" %in% databaser::db_list_variables(con, "sites") &&
      "data_source" %in% databaser::db_list_variables(con, "processes")) {
    update_sites_data_sources(con, site = site)
  }
  
  return(invisible(con))
  
}


update_sites_observation_counts <- function(con, site = NA) {
  
  # Get processes counts
  df <- databaser::db_get(
    con, 
    "SELECT 
    site, 
    observation_count
    FROM processes
    ORDER BY site"
  )
  
  # Filter to sites
  if (!is.na(site[1])) df <- filter(df, site %in% !!site)
  
  # Summarise
  df <- df %>% 
    group_by(site) %>% 
    summarise(observation_count = sum(observation_count, na.rm = TRUE)) %>% 
    ungroup()
  
  # Update variable to be null before insert
  if (!is.na(site[1])) {
    
    site_collapsed <- site %>% 
      stringr::str_c("'", ., "'") %>% 
      stringr::str_c(collapse = ",")
    
    sql_set_null <- stringr::str_c(
      "UPDATE sites
      SET observation_count = NULL
      WHERE site IN (", site_collapsed, ")"
    ) 
    
  } else {
    
    # All sites
    sql_set_null <- "
      UPDATE sites 
      SET observation_count = NULL
    "
    
  }
  
  # Use statements
  sql_set_null %>% 
    stringr::str_squish() %>% 
    databaser::db_execute(con, .)
  
  # Update variable
  df %>% 
    databaser::build_update_statements("sites", ., where = "site", squish = TRUE) %>% 
    databaser::db_execute(con, .)
  
  return(invisible(con))
  
}


update_sites_data_sources <- function(con, site = NA) {
    
  # Build sql query
  sql <- "
    SELECT site, 
    data_source 
    FROM processes 
  "
  
  if (!is.na(site[1])) {
    
    # Format site for sql
    site <- site %>% 
      stringr::str_c("'", ., "'") %>% 
      stringr::str_c(collapse = ",")
    
    sql_where <- stringr::str_c(
      "WHERE site IN (", site, ")"
    )
    
    # Add where clause
    sql <- stringr::str_c(sql, sql_where)
    
  } 
  
  # Add order by, clean, and use
  df <- sql %>% 
    stringr::str_c(" ORDER BY site") %>% 
    stringr::str_squish() %>% 
    databaser::db_get(con, .)
  
  # Summarise data sources
  df <- df %>% 
    group_by(site) %>% 
    summarise(data_source = stringr::str_c(unique(data_source), collapse = "; ")) %>% 
    ungroup()
  
  # Build update statements and use
  df %>%
    databaser::build_update_statements("sites", ., where = "site", squish = TRUE) %>%
    databaser::db_execute(con, .)
  
  return(invisible(con))
  
}

