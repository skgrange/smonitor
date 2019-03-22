#' Function to update the \code{variables_monitored} variable in the 
#' \code{`site`} table of an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param site Vector of sites to update. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
update_variables_monitored <- function(con, site = NA) {
  
  # Get process entries
  df <- databaser::db_get(
    con, 
    "SELECT site,
    variable, 
    observation_count
    FROM processes"
  ) 
  
  # Filter to sites
  if (!is.na(site[1])) df <- filter(df, site %in% !!site)
  
  # Summarise
  df <- df %>% 
    arrange(site,
            variable) %>% 
    group_by(site) %>% 
    summarise(variables_monitored = stringr::str_c(unique(variable), collapse = "; "),
              observation_count = sum(observation_count, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(variables_monitored = if_else(
      observation_count == 0, 
      NA_character_, variables_monitored
    ),
    variables_monitored = stringr::str_replace_na(variables_monitored))
  
  # Build some sql and use
  df %>% 
    databaser::build_update_statements("sites", ., where = "site", squish = TRUE) %>% 
    databaser::db_execute(con, .)
  
  return(invisible(con))
  
}
