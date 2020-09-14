#' Function to invalidate wind direction processes which are less than \code{0}
#' or greater than \code{360} degrees in an \code{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param print_query Should the SQL queries be printed? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible. 
#' 
#' @export
invalidate_bad_wind_direction <- function(con, print_query = FALSE) {
  
  # Get process keys
  process <- databaser::db_get(
    con, 
    "SELECT process
    FROM processes 
    WHERE variable = 'wd'"
  )[, 1]
  
  process <- stringr::str_c(process, collapse = ",")
  
  sql_low <- stringr::str_c(
    "UPDATE observations 
    SET validity=0
    WHERE process IN (", process, ")
    AND value<0"
  )
  
  sql_high <- stringr::str_c(
    "UPDATE observations 
    SET validity=0
    WHERE process IN (", process, ")
    AND value>360"
  )
  
  # Combine
  sql <- c(sql_low, sql_high)
  
  # Clean
  sql <- stringr::str_squish(sql)
  
  # Print
  if (print_query) message(sql)
  
  # Do
  databaser::db_execute(con, sql)
  
}
