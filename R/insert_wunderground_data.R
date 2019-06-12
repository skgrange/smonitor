#' Function to get observations from wunderground and insert them into a 
#' \strong{smonitor} database. 
#' 
#' Site-variable combinations need to be present in the database's process table,
#' otherwise they will be silently filtered and not be inserted. New, downloaded
#' observations will take priority over those stored in the database and old 
#' observations are deleted with this function. 
#' 
#' @param con Database connection. 
#' 
#' @param site Wunderground site code. 
#' 
#' @param start Start date to download and insert. 
#' 
#' @param end End date to download and insert. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Invisible \code{con}
#' 
#' @author Stuart K. Grange
#' 
#' @export
insert_wunderground_data <- function(con, site, start, end = NA, verbose = FALSE) {
  
  # Get look-up table for a join
  df_look <- import_processes(con) %>% 
    filter(site %in% !!site) %>% 
    select(process,
           site,
           variable)
  
  # Get observations, scrape_wunderground is not vectorised over site
  df <- purrr::map_dfr(
    site, 
    ~sscraper::scrape_wunderground(
      ., 
      start = start, 
      end = end, 
      verbose = verbose
    )
  )
  
  if (nrow(df) != 0) {
    
    # Transform and reshape data
    df <- df %>% 
      select(-date, 
             -date_local, 
             -software) %>% 
      rename(date = date_unix) %>% 
      tidyr::gather(variable, value, -date, -site) %>% 
      mutate(date_end = NA, 
             validity = NA,
             summary = 0L)
    
    # Join processes, only processes in table will be kept, then arrange
    df <- df %>% 
      inner_join(df_look, by = c("site", "variable")) %>% 
      select(date,
             date_end,
             process,
             summary,
             validity,
             value)
    
    if (nrow(df) > 0) {
      delete_observations(con, df, match = "between", verbose = verbose)
      insert_observations(con, df, verbose = verbose)
    } else {
      message(threadr::date_message(), "No data inserted...")
    }
    
  } else {
    message("No data was returned from the API...")
  }
  
  return(invisible(con))
  
}
