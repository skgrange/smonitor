#' Function to delete observations from an \code{`observations`} table in a 
#' database. 
#' 
#' \code{delete_observations} is generally used immediately before a database 
#' insert so old observations are deleted before a new set of observations are 
#' inserted. This process avoids duplicate observations which are almost always
#' undesirable. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection.
#' 
#' @param df Data frame; usually an object which is about to be inserted into 
#' a database table. 
#' 
#' @param match Type of match to use for dates. Currently only \code{"between"}
#' is supported. 
#' 
#' @export
delete_observations <- function(con, df, match = "between") {
  
  # Drop tbl_df
  df <- threadr::base_df(df)
  
  if (match == "between") {
    
    # Get keys
    process <- df$process[1]
    summary <- df$summary[1]
    
    # Get dates, to-do: is avoiding nas ok? 
    date_min <- min(df$date, na.rm = TRUE)
    date_max <- max(df$date, na.rm = TRUE)
    
    # Build statement
    sql <- stringr::str_c(
      "DELETE FROM observations 
      WHERE process = ", process, 
      " AND summary = ", summary, 
      " AND date BETWEEN ", date_min, " AND ", date_max)
    
    # Clean
    sql <- threadr::str_trim_many_spaces(sql)
    
    # Use statement
    threadr::db_send(con, sql)
    
  }
  
  # No return
  
}
