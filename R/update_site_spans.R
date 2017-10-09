#' Function to update \code{date_start} and \code{date_end}, for sites and then
#' update the \code{`sites`} table for an \strong{smonitor} database.
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
#' @return Invisible. 
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
    mutate(date_start = as.numeric(date_start),
           date_end = as.numeric(date_end))
  
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
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Update variables to be null before insert
  databaser::db_execute(
    con, 
    "UPDATE sites SET date_start = NULL, date_end = NULL"
  )
  
  # Use statements
  databaser::db_execute(con, sql)
  
  # Also update variables monitored
  if (variables_monitored) update_variables_monitored(con)
  
  # No return
  
}
