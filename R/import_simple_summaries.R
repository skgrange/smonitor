#' Function to import simple summaries from an \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
#' 
#' @param site A vector of sites to import. If not used, all sites are selected
#' and returned. 
#' 
#' @param summary Summaries to import, can either be \code{"month"} or 
#' \code{"year"}.
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
import_simple_summaries <- function(con, site = NA, summary = "month",
                                    date_insert = FALSE, tz = "UTC") {
  
  # Check and parse inputs
  summary <- stringr::str_to_lower(summary)
  stopifnot(summary %in% c("month", "year"))
  
  # For database's integer keys
  summary_smonitor <- if_else(summary == "month", 92L, NA_integer_)
  summary_smonitor <- if_else(summary == "year", 102L, summary_smonitor)
  
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
    WHERE summary = ", summary_smonitor, " 
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
  
  # Add variable to statement
  if (date_insert) {
    sql <- stringr::str_replace(sql, "date,", "date_insert, date,")
  }
  
  # Clean statement
  sql <- stringr::str_squish(sql)
  
  # Query database
  df <- databaser::db_get(con, sql) %>% 
    mutate(date = threadr::parse_unix_time(date, tz = tz),
           date_end = threadr::parse_unix_time(date_end, tz = tz))
  
  # Parse date insert
  if (date_insert) {
    df <- mutate(df, date_insert = threadr::parse_unix_time(date_insert, tz = tz))
  }
  
  return(df)
  
}
