#' Function to update the \code{variables_monitored} variable in the 
#' \code{`site`} table of an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible. 
#' 
#' @export
update_variables_monitored <- function(con) {
  
  # Summarise process table
  df <- import_processes(con) %>% 
    arrange(site,
            variable) %>% 
    group_by(site) %>% 
    summarise(variables_monitored = stringr::str_c(unique(variable), collapse = "; "),
              observation_count = sum(observation_count, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(variables_monitored = ifelse(observation_count == 0, NA, variables_monitored),
           variables_monitored = stringr::str_replace_na(variables_monitored))
  
  # Build some sql
  sql_update <- stringr::str_c(
    "UPDATE sites SET variables_monitored='", 
    df$variables_monitored, "'
    WHERE site='", 
    df$site, "'"
  )
  
  # Clean
  sql_update <- stringr::str_replace_all(sql_update, "'NA'", "NULL")
  sql_update <- stringr::str_squish(sql_update)
  
  # Do
  databaser::db_execute(con, sql_update)
  
  # No return
  
}
