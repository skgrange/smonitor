#' Function to insert simple summaries into a \strong{smonitor} database. 
#' 
#' \code{insert_simple_summaries} will delete old summaries before inserting. 
#' 
#' @param con Database connection to a \strong{smonitor} database. 
#' 
#' @param df Tibble from \code{calculate_simple_summaries}. 
#' 
#' @param progress Should a progress bar be displayed? 
#' 
#' @author Stuart K. Grange. 
#' 
#' @return Invisible \code{con}. 
#' 
#' @export
insert_simple_summaries <- function(con, df, progress = FALSE) {
  
  # Check database for table
  stopifnot(databaser::db_table_exists(con, "observations_simple_summaries"))
  
  # Make dates numeric and split df into a list 
  list_df <- df %>% 
    mutate(date = as.numeric(date),
           date_end = as.numeric(date_end)) %>% 
    bind_rows(databaser::db_table_names(con, "observations_simple_summaries"), .) %>% 
    dplyr::group_split(site,
                       variable,
                       summary_source,
                       summary)
  
  # Delete and insert every element in list
  purrr::walk(
    list_df,
    ~insert_simple_summaries_worker(
      con = con, 
      df = .x
    ),
    .progress = progress
  )
  
  return(invisible(con))
  
}


insert_simple_summaries_worker <- function(con, df) {
  
  # Delete and insert with a transaction
  databaser::db_with_transaction(
    con, 
    {
      
      # Delete old summaries based on input tibble
      df %>% 
        build_simple_summaries_delete_sql() %>% 
        databaser::db_execute(con, .)
      
      # Insert into database
      df %>% 
        mutate(date_insert = lubridate::now(), 
               date_insert = as.numeric(date_insert),
               date_insert = round(date_insert)) %>% 
        databaser::db_insert(con, "observations_simple_summaries", ., replace = FALSE)
      
    }
  )
  
  return(invisible(con))
  
}


build_simple_summaries_delete_sql <- function(df) {
  
  # Get date range for WHERE clause
  date_range <- range(df$date)
  
  # Build delete statement
  sql <- stringr::str_c(
    "DELETE FROM observations_simple_summaries 
    WHERE site='", df$site[1], "' 
    AND variable='", df$variable[1], "'
    AND summary_source=", df$summary_source[1],
    " AND summary=", df$summary[1],
    " AND date BETWEEN ", date_range[1], 
    " AND ", date_range[2]
  ) %>% 
    stringr::str_squish()
  
  return(sql)
  
}
