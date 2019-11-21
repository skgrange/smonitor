#' Function to import simple summaries from an \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
#' 
#' @param site A vector of sites to import. If not used, all sites are selected
#' and returned. 
#' 
#' @param summary Summaries to import, can either be \code{"monthly_means"} or 
#' \code{"annual_means"}.
#' 
#' @param date_insert Should the \code{date_insert} variable be imported? 
#' 
#' @param tz What time zone should the dates be returned as? 
#' 
#' @return Tibble. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_simple_summaries <- function(con, site = NA, summary = "monthly_means",
                                    date_insert = FALSE, tz = "UTC") {
  
  # To-do: add variable argument? 
  
  # Check if database table exists
  databaser::db_table_exists(con, "observations_simple_summaries")
  
  # Check and constrain summary
  summary <- summary %>% 
    stringr::str_trim() %>% 
    stringr::str_to_lower() %>% 
    stringr::str_replace("_mean$", "_means") %>% 
    if_else(. == "yearly_means", "annual_means", .)
  
  stopifnot(summary %in% c("annual_means", "monthly_means"))
  stopifnot(length(summary) == 1L)
  
  # For database's integer keys
  if (summary == "monthly_means") {
    summary_integer <- 92L
  } else if (summary == "annual_means") {
    summary_integer <- 102L
  }
  
  # Build sql
  sql <- stringr::str_c(
    "SELECT date,
    date_end,
    site,
    variable,
    summary_source,
    summary,
    count,
    value 
    FROM observations_simple_summaries
    WHERE summary=", summary_integer, " 
    ORDER BY site, 
    variable"
  )
  
  # Add site to statement
  if (!is.na(site[1])) {
    
    # Collapse site
    site <- site %>% 
      stringr::str_c("'", ., "'") %>% 
      stringr::str_c(collapse = ",")
    
    # Add site where clause
    sql <- stringr::str_replace(
      sql, 
      "ORDER", 
      stringr::str_c("AND site IN (", site, ") ORDER")
    )
    
  }
  
  # Add date_insert to statement
  if (date_insert) {
    sql <- stringr::str_replace(sql, "date,", "date_insert, date,")
  }
  
  # Query database
  df <- sql %>% 
    stringr::str_squish() %>% 
    databaser::db_get(con, .) %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz),
           date_end = threadr::parse_unix_time(date_end, tz = tz))
  
  # Parse date insert
  if (date_insert) {
    df <- mutate(df, date_insert = threadr::parse_unix_time(date_insert, tz = tz))
  }
  
  return(df)
  
}
