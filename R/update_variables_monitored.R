#' Function to update the \code{variables_monitored} variable in the 
#' \code{`process`} table of an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @import dplyr
#' 
#' @export
update_variables_monitored <- function(con) {
  
  # Get look up table
  df_processes <- import_processes(con)
  
  # Nest variables
  df <- df_processes %>% 
    group_by(site) %>% 
    do(nest_variable_vectors(.)) %>% 
    ungroup() %>% 
    mutate(variables_monitored = ifelse(
      observation_count == 0, NA, variables_monitored),
      variables_monitored = stringr::str_replace_na(variables_monitored))
  
  # Build some sql
  sql <- stringr::str_c(
    "UPDATE sites SET variables_monitored='", df$variables_monitored, "'
    WHERE site='", df$site, "'")
  
  sql <- stringr::str_replace_all(sql, "'NA'", "NULL")
  sql <- threadr::str_trim_many_spaces(sql)
  
  # Do
  db_execute(con, sql)
  
}


# Nest the vector
nest_variable_vectors <- function(df) {
  
  variables <- sort(unique(df$variable))
  variables <- stringr::str_c(variables, collapse = "; ")
  observation_count <- sum(df$observation_count, na.rm = TRUE)
  
  df <- data.frame(
    site = df$site[1],
    variables_monitored = variables,
    observation_count = observation_count,
    stringsAsFactors = FALSE
  )
  
  # Return
  df
  
}
