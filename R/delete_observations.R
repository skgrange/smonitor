#' Function to delete observations from the \code{`observations`} table in a
#' \strong{smonitor} database. 
#' 
#' \code{delete_observations} is generally used immediately before a database 
#' insert so old observations are deleted before a new set of observations are 
#' inserted. This process avoids duplicate observations which are almost always
#' undesirable. 
#' 
#' \code{delete_observations} allows the use of groups based on the input data
#' frame.
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection.
#' 
#' @param df Data frame; usually an object which is about to be inserted into 
#' a database table. \code{delete_observations} uses this as a mapping table to
#' know what to send the database in the delete statement.
#' 
#' @param match Type of match to use for dates. \code{"between"} and 
#' \code{"all"} are supported. Beware that \code{"all"} will remove all 
#' observations associated with a process. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @return Invisible, database connection. 
#' 
#' @seealso \code{\link{db_execute}}, \code{\link{insert_observations}}
#' 
#' @export
delete_observations <- function(con, df, match = "between", verbose = FALSE) {
  
  # May need to use the argument
  if (any(is.na(df$process)))
    stop("Input data frame must not contain missing processes...", call. = FALSE)
  
  if (any(is.na(df$date)))
    stop("Input data frame must not contain missing dates...", call. = FALSE)
  
  if (match == "between") {
    
    # Delete observations by groups
    df %>% 
      split(.$process, .$summary) %>% 
      purrr::walk(
        ~delete_observations_worker(
          con, 
          df = .x,
          verbose = verbose
        )
      )
    
  } else if (match == "all") {
    
    # Get processes
    keys <- unique(df$process)
    
    # Delete all observations associated with a process
    delete_process(con, keys)
    
  }
  
  return(invisible(con))
  
}


# No export
delete_observations_worker <- function(con, df, verbose) {
  
  # Get keys
  process <- df$process[1]
  summary <- df$summary[1]
  
  # Get dates
  date_min <- min(df$date, na.rm = TRUE)
  date_max <- max(df$date, na.rm = TRUE)
  
  # Convert if needed
  if (lubridate::is.POSIXt(date_min)) date_min <- as.numeric(date_min)
  if (lubridate::is.POSIXt(date_max)) date_max <- as.numeric(date_max)
  
  # Build statement
  sql <- stringr::str_c(
    "DELETE FROM observations 
      WHERE process = ", process, 
    " AND summary = ", summary, 
    " AND date BETWEEN ", date_min, " AND ", date_max
  ) %>% 
    stringr::str_squish()
  
  # Message to user
  if (verbose) {
    
    # Clean dates
    date_min_format <- threadr::parse_unix_time(date_min)
    date_max_format <- threadr::parse_unix_time(date_max)
    date_min_format <- format(date_min_format, usetz = TRUE)
    date_max_format <- format(date_max_format, usetz = TRUE)
    
    stringr::str_c(
      threadr::date_message(), 
      "Deleting observations from `process` ", process,
      " and `summary` ", summary, 
      " between ", date_min_format, 
      " and ", date_max_format
    ) %>% 
      message()
    
  }
  
  # Use statement
  databaser::db_execute(con, sql)
  
  # No return
  
}
